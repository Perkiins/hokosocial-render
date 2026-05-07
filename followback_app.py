import datetime
import json
import logging
import os
import sqlite3
import threading
import time
from functools import wraps

import jwt
from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.security import check_password_hash, generate_password_hash

# stripe es opcional: si no hay STRIPE_SECRET_KEY el módulo se carga pero los
# endpoints /api/checkout y /api/stripe/webhook devuelven 503.
try:
    import stripe as stripe_lib
except ImportError:
    stripe_lib = None

# --- Config ---
app = Flask(__name__)

SECRET_KEY = os.environ.get('SECRET_KEY')
if not SECRET_KEY:
    SECRET_KEY = 'dev-only-not-for-production'
    app.logger.warning('SECRET_KEY no está definido en entorno — usando fallback dev.')
app.config['SECRET_KEY'] = SECRET_KEY

WORKER_API_KEY = os.environ.get('WORKER_API_KEY', '')
if not WORKER_API_KEY:
    app.logger.warning('WORKER_API_KEY no definido — endpoints /api/worker rechazarán todo.')

STRIPE_SECRET_KEY = os.environ.get('STRIPE_SECRET_KEY', '')
STRIPE_WEBHOOK_SECRET = os.environ.get('STRIPE_WEBHOOK_SECRET', '')
STRIPE_ENABLED = bool(STRIPE_SECRET_KEY and stripe_lib)
if STRIPE_ENABLED:
    stripe_lib.api_key = STRIPE_SECRET_KEY
else:
    app.logger.info('Stripe no configurado — endpoints de pago devolverán 503.')

FRONTEND_URL = os.environ.get('FRONTEND_URL', 'https://hokosocial.vercel.app').rstrip('/')

cors_origins = [o.strip() for o in os.environ.get('CORS_ORIGINS', 'https://hokosocial.vercel.app').split(',') if o.strip()]
CORS(app, origins=cors_origins, supports_credentials=True)

DB_PATH = os.environ.get('DB_PATH', 'usuarios.db')

TASK_TYPES = ('search', 'followback', 'simulate', 'instagram_profile',
              'ig_snapshot', 'footprint_scan', 'ig_growth_discover',
              'ig_growth_view_stories')
WORKER_TYPES = ('search', 'followback', 'instagram_profile',
                'ig_snapshot', 'ig_growth_discover', 'ig_growth_view_stories')
# `footprint_scan` corre en el propio backend (thread), no requiere worker en PC.
MAX_LOG_LINES = 2000
WORKER_OFFLINE_AFTER = 30
ZOMBIE_TASK_AFTER = 300  # 5 min sin update => zombie, se auto-failea

TERMS_VERSION = '2026-04-25'

# Catálogo seed de paquetes (idempotente: si ya existen por slug, no duplica).
DEFAULT_PACKS = [
    {'slug': 'starter', 'name': 'Starter', 'tokens': 50, 'price_cents': 499, 'currency': 'eur',
     'description': '50 ejecuciones — para probar.'},
    {'slug': 'pro', 'name': 'Pro', 'tokens': 200, 'price_cents': 1499, 'currency': 'eur',
     'description': '200 ejecuciones — el más popular.'},
    {'slug': 'max', 'name': 'Max', 'tokens': 500, 'price_cents': 2999, 'currency': 'eur',
     'description': '500 ejecuciones — para power users.'},
]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
log = logging.getLogger('hokosocial')


# --- DB ---
def get_db():
    # check_same_thread=False permite al orchestrator (thread daemon) abrir
    # conexiones desde otro hilo. Cada llamada crea su propia conexión, así
    # que no hay conflicto de uso compartido — solo desactivamos el guard.
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def now_iso() -> str:
    return datetime.datetime.utcnow().isoformat(timespec='seconds') + 'Z'


def _try_alter(conn, sql: str):
    """Intenta un ALTER TABLE; ignora si la columna ya existe."""
    try:
        conn.execute(sql)
    except sqlite3.OperationalError as e:
        if 'duplicate column' not in str(e).lower():
            raise


def init_db():
    with get_db() as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS usuarios (
            username TEXT PRIMARY KEY,
            password TEXT,
            tokens INTEGER DEFAULT 5,
            rol TEXT DEFAULT 'user'
        )''')
        # Migration: terms_accepted_at + stripe_customer_id
        _try_alter(conn, 'ALTER TABLE usuarios ADD COLUMN terms_accepted_at TEXT')
        _try_alter(conn, 'ALTER TABLE usuarios ADD COLUMN stripe_customer_id TEXT')

        conn.execute('''CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner TEXT NOT NULL,
            type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            created_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            error TEXT,
            log TEXT NOT NULL DEFAULT ''
        )''')
        # Migration: payload + result (JSON serializado)
        _try_alter(conn, 'ALTER TABLE tasks ADD COLUMN payload TEXT')
        _try_alter(conn, 'ALTER TABLE tasks ADD COLUMN result TEXT')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks (status, created_at)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_tasks_owner ON tasks (owner, created_at)')

        conn.execute('''CREATE TABLE IF NOT EXISTS worker_status (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_seen TEXT,
            current_task_id INTEGER
        )''')
        conn.execute('INSERT OR IGNORE INTO worker_status (id) VALUES (1)')

        conn.execute('''CREATE TABLE IF NOT EXISTS token_packs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            tokens INTEGER NOT NULL,
            price_cents INTEGER NOT NULL,
            currency TEXT NOT NULL DEFAULT 'eur',
            is_active INTEGER NOT NULL DEFAULT 1
        )''')
        # Seed de packs (idempotente por slug)
        for p in DEFAULT_PACKS:
            conn.execute(
                'INSERT OR IGNORE INTO token_packs (slug, name, description, tokens, price_cents, currency) '
                'VALUES (?, ?, ?, ?, ?, ?)',
                (p['slug'], p['name'], p['description'], p['tokens'], p['price_cents'], p['currency']),
            )

        conn.execute('''CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner TEXT NOT NULL,
            kind TEXT NOT NULL,
            tokens_delta INTEGER NOT NULL,
            balance_after INTEGER NOT NULL,
            note TEXT,
            stripe_session_id TEXT,
            created_at TEXT NOT NULL
        )''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_tx_owner ON transactions (owner, created_at)')
        conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_tx_stripe ON transactions (stripe_session_id) '
                     "WHERE stripe_session_id IS NOT NULL")

        conn.execute('''CREATE TABLE IF NOT EXISTS ig_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner TEXT NOT NULL,
            ig_username TEXT NOT NULL,
            taken_at TEXT NOT NULL,
            followers_count INTEGER NOT NULL DEFAULT 0,
            following_count INTEGER NOT NULL DEFAULT 0,
            mutuals_count INTEGER NOT NULL DEFAULT 0,
            not_following_back_count INTEGER NOT NULL DEFAULT 0,
            fans_count INTEGER NOT NULL DEFAULT 0,
            profile_json TEXT,
            followers_json TEXT,
            following_json TEXT,
            task_id INTEGER
        )''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_ig_snap_owner '
                     'ON ig_snapshots (owner, ig_username, taken_at)')

        conn.execute('''CREATE TABLE IF NOT EXISTS ig_growth_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner TEXT NOT NULL,
            kind TEXT NOT NULL,           -- 'hashtag' | 'competitor' | 'post'
            value TEXT NOT NULL,          -- hashtag sin #, username sin @, o shortcode
            niche TEXT,                   -- 'fitness' | 'gaming' | 'anime' | 'moto' | 'mixed' | NULL
            priority INTEGER NOT NULL DEFAULT 5,  -- 1=alta, 10=baja
            enabled INTEGER NOT NULL DEFAULT 1,
            last_run_at TEXT,
            candidates_found INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_ig_growth_sources_owner '
                     'ON ig_growth_sources (owner, enabled, priority)')
        conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_ig_growth_sources_unique '
                     'ON ig_growth_sources (owner, kind, value)')

        conn.execute('''CREATE TABLE IF NOT EXISTS ig_growth_candidates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner TEXT NOT NULL,
            ig_pk TEXT NOT NULL,
            ig_username TEXT NOT NULL,
            full_name TEXT,
            profile_pic_url TEXT,
            biography TEXT,
            followers_count INTEGER,
            following_count INTEGER,
            posts_count INTEGER,
            is_private INTEGER NOT NULL DEFAULT 0,
            is_verified INTEGER NOT NULL DEFAULT 0,
            score INTEGER NOT NULL DEFAULT 0,
            score_breakdown TEXT,         -- JSON: {key: weight}
            status TEXT NOT NULL DEFAULT 'pending',
              -- pending | engaged | followed | unfollowed | converted | skipped | excluded
            source_id INTEGER,
            source_kind TEXT,
            source_value TEXT,
            niche TEXT,
            discovered_at TEXT NOT NULL,
            last_action_at TEXT,
            FOREIGN KEY(source_id) REFERENCES ig_growth_sources(id)
        )''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_ig_growth_cand_owner '
                     'ON ig_growth_candidates (owner, status, score DESC)')
        conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_ig_growth_cand_unique '
                     'ON ig_growth_candidates (owner, ig_pk)')

        conn.execute('''CREATE TABLE IF NOT EXISTS ig_growth_settings (
            owner TEXT PRIMARY KEY,
            auto_enabled INTEGER NOT NULL DEFAULT 0,
            daily_view_stories_limit INTEGER NOT NULL DEFAULT 50,
            daily_follow_limit INTEGER NOT NULL DEFAULT 30,
            daily_like_limit INTEGER NOT NULL DEFAULT 60,
            discovery_interval_minutes INTEGER NOT NULL DEFAULT 180,
            engagement_interval_minutes INTEGER NOT NULL DEFAULT 25,
            min_score INTEGER NOT NULL DEFAULT 60,
            active_hours_start INTEGER NOT NULL DEFAULT 8,
            active_hours_end INTEGER NOT NULL DEFAULT 23,
            last_discovery_at TEXT,
            last_engagement_at TEXT,
            paused_until TEXT,
            paused_reason TEXT,
            updated_at TEXT
        )''')

        conn.execute('''CREATE TABLE IF NOT EXISTS app_locks (
            name TEXT PRIMARY KEY,
            holder TEXT,
            lease_until TEXT
        )''')

        conn.execute('''CREATE TABLE IF NOT EXISTS ig_growth_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner TEXT NOT NULL,
            candidate_id INTEGER,
            ig_pk TEXT,
            action TEXT NOT NULL,         -- view_story | like | follow | unfollow | comment | dm
            success INTEGER NOT NULL DEFAULT 1,
            error TEXT,
            created_at TEXT NOT NULL
        )''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_ig_growth_actions_rate '
                     'ON ig_growth_actions (owner, action, created_at)')

        conn.execute('''CREATE TABLE IF NOT EXISTS footprint_scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner TEXT NOT NULL,
            created_at TEXT NOT NULL,
            email_hash TEXT,
            phone_hash TEXT,
            email_masked TEXT,
            phone_masked TEXT,
            face_used INTEGER NOT NULL DEFAULT 0,
            score INTEGER,
            result_json TEXT NOT NULL,
            task_id INTEGER
        )''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_footprint_owner '
                     'ON footprint_scans (owner, created_at)')

        conn.execute('''CREATE TABLE IF NOT EXISTS bot_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            cookies TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            added_at TEXT NOT NULL,
            last_used_at TEXT,
            used_count INTEGER NOT NULL DEFAULT 0,
            burn_reason TEXT,
            burned_at TEXT,
            notes TEXT
        )''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_bot_status ON bot_accounts (status, last_used_at)')

        conn.commit()


def bootstrap_admin():
    """En cada arranque garantiza que el owner del servicio tenga su cuenta
    admin con un saldo mínimo. Útil mientras el filesystem de Render es
    efímero — sobrevive a redeploys.

    Variables de entorno:
    - BOOTSTRAP_ADMIN_USERNAME: si existe el user, se promueve a admin.
    - BOOTSTRAP_ADMIN_PASSWORD (opcional): si la cuenta no existe, se crea
      con esta password (hasheada). Si el user ya existe NO se toca su pwd.
    - BOOTSTRAP_ADMIN_TOKENS (opcional, default 0): asegura que el saldo sea
      al menos N. Si está más bajo, se eleva hasta N (y se registra en
      transactions kind='grant' para auditoría).
    """
    target = os.environ.get('BOOTSTRAP_ADMIN_USERNAME', '').strip()
    if not target:
        return
    boot_pwd = os.environ.get('BOOTSTRAP_ADMIN_PASSWORD', '')
    try:
        boot_min_tokens = int(os.environ.get('BOOTSTRAP_ADMIN_TOKENS', '0') or 0)
    except ValueError:
        boot_min_tokens = 0

    with get_db() as conn:
        row = conn.execute(
            'SELECT username, tokens, rol FROM usuarios WHERE username = ?',
            (target,),
        ).fetchone()

        # 1) Crear si no existe (solo si tenemos password)
        if not row and boot_pwd:
            try:
                conn.execute(
                    'INSERT INTO usuarios (username, password, rol, tokens, terms_accepted_at) '
                    'VALUES (?, ?, ?, ?, ?)',
                    (target, hash_password(boot_pwd), 'admin',
                     max(boot_min_tokens, 0), now_iso()),
                )
                row2 = conn.execute('SELECT tokens FROM usuarios WHERE username = ?', (target,)).fetchone()
                add_transaction(conn, target, 'grant', row2['tokens'], row2['tokens'],
                                note='Bootstrap admin (user creado automáticamente)')
                conn.commit()
                log.info('bootstrap_admin: %s creado con %d tokens', target, row2['tokens'])
                return
            except Exception:
                log.exception('bootstrap_admin: fallo creando %s', target)
                return
        if not row:
            log.warning('bootstrap_admin: %s no existe y no hay BOOTSTRAP_ADMIN_PASSWORD para crearlo', target)
            return

        # 2) Promover a admin si no lo es
        if row['rol'] != 'admin':
            conn.execute('UPDATE usuarios SET rol = ? WHERE username = ?', ('admin', target))
            log.info('bootstrap_admin: %s promovido a admin', target)

        # 3) Asegurar saldo mínimo
        if boot_min_tokens > 0 and (row['tokens'] or 0) < boot_min_tokens:
            delta = boot_min_tokens - (row['tokens'] or 0)
            conn.execute('UPDATE usuarios SET tokens = ? WHERE username = ?',
                         (boot_min_tokens, target))
            add_transaction(conn, target, 'grant', delta, boot_min_tokens,
                            note=f'Bootstrap admin (saldo asegurado {boot_min_tokens})')
            log.info('bootstrap_admin: %s saldo elevado a %d (+%d)', target, boot_min_tokens, delta)

        conn.commit()


init_db()
# bootstrap_admin() se llama al final del módulo (después de definir
# hash_password y add_transaction).


# --- Password hashing ---
def hash_password(plain: str) -> str:
    return generate_password_hash(plain)


def verify_password(plain: str, stored: str) -> tuple[bool, bool]:
    if not stored:
        return False, False
    if stored.startswith(('pbkdf2:', 'scrypt:', 'argon2')):
        return check_password_hash(stored, plain), False
    return plain == stored, True


# --- JWT ---
def generar_token(username: str) -> str:
    payload = {
        'username': username,
        'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=12),
    }
    return jwt.encode(payload, app.config['SECRET_KEY'], algorithm='HS256')


def verificar_token(token: str):
    try:
        return jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError as e:
        log.warning('JWT inválido: %s', e)
        return None


def get_user(username: str):
    with get_db() as conn:
        row = conn.execute(
            'SELECT username, password, tokens, rol, terms_accepted_at, stripe_customer_id '
            'FROM usuarios WHERE username = ?',
            (username,),
        ).fetchone()
        return dict(row) if row else None


def require_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        token = request.headers.get('Authorization', '').replace('Bearer ', '').strip()
        payload = verificar_token(token)
        if not payload:
            return jsonify({'message': 'Token inválido'}), 401
        user = get_user(payload['username'])
        if not user:
            return jsonify({'message': 'Usuario no encontrado'}), 401
        request.user = user
        return fn(*args, **kwargs)
    return wrapper


def require_admin(fn):
    @wraps(fn)
    @require_auth
    def wrapper(*args, **kwargs):
        if request.user.get('rol') != 'admin':
            return jsonify({'message': 'Acceso restringido'}), 403
        return fn(*args, **kwargs)
    return wrapper


def require_worker(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not WORKER_API_KEY:
            return jsonify({'message': 'Worker auth no configurada en el servidor'}), 503
        provided = request.headers.get('X-Worker-Key', '').strip()
        if not provided or provided != WORKER_API_KEY:
            return jsonify({'message': 'Worker no autorizado'}), 401
        return fn(*args, **kwargs)
    return wrapper


# --- Wallet helpers (transactions) ---
def add_transaction(conn, owner: str, kind: str, tokens_delta: int, balance_after: int,
                    note: str | None = None, stripe_session_id: str | None = None) -> int:
    """Crea una fila en transactions. Reusa la conexión del caller."""
    cur = conn.execute(
        'INSERT INTO transactions (owner, kind, tokens_delta, balance_after, note, stripe_session_id, created_at) '
        'VALUES (?, ?, ?, ?, ?, ?, ?)',
        (owner, kind, tokens_delta, balance_after, note, stripe_session_id, now_iso()),
    )
    return cur.lastrowid


def credit_tokens(username: str, amount: int, kind: str, note: str | None = None,
                  stripe_session_id: str | None = None) -> tuple[bool, int]:
    """Suma `amount` tokens a `username`. Idempotente respecto a stripe_session_id."""
    if amount <= 0:
        return False, 0
    with get_db() as conn:
        if stripe_session_id:
            existing = conn.execute(
                'SELECT id FROM transactions WHERE stripe_session_id = ?',
                (stripe_session_id,),
            ).fetchone()
            if existing:
                row = conn.execute(
                    'SELECT tokens FROM usuarios WHERE username = ?', (username,)
                ).fetchone()
                return False, (row['tokens'] if row else 0)
        conn.execute(
            'UPDATE usuarios SET tokens = tokens + ? WHERE username = ?',
            (amount, username),
        )
        row = conn.execute(
            'SELECT tokens FROM usuarios WHERE username = ?', (username,)
        ).fetchone()
        if not row:
            return False, 0
        balance = row['tokens']
        add_transaction(conn, username, kind, amount, balance, note, stripe_session_id)
        conn.commit()
    log.info('credited %d tokens to %s (kind=%s, balance=%d)', amount, username, kind, balance)
    return True, balance


def consume_n_tokens(username: str, n: int, note: str | None = None) -> tuple[bool, int]:
    """Decrementa N tokens + crea transaction kind='consume'. Atómico."""
    if n <= 0:
        with get_db() as conn:
            row = conn.execute('SELECT tokens FROM usuarios WHERE username = ?', (username,)).fetchone()
        return True, (row['tokens'] if row else 0)
    with get_db() as conn:
        cur = conn.execute(
            'UPDATE usuarios SET tokens = tokens - ? WHERE username = ? AND tokens >= ?',
            (n, username, n),
        )
        row = conn.execute(
            'SELECT tokens FROM usuarios WHERE username = ?', (username,)
        ).fetchone()
        balance = row['tokens'] if row else 0
        if cur.rowcount > 0:
            add_transaction(conn, username, 'consume', -n, balance, note=note)
            conn.commit()
            return True, balance
        conn.commit()
    return False, balance


def consume_one_token(username: str, note: str | None = None) -> tuple[bool, int]:
    """Compat — usa consume_n_tokens(1)."""
    return consume_n_tokens(username, 1, note)


# --- Auth endpoints ---
@app.route('/api/register', methods=['POST'])
def register():
    data = request.get_json(silent=True) or {}
    username = (data.get('username') or '').strip()
    password = data.get('password') or ''
    accepted_terms = bool(data.get('accept_terms'))

    if not username or not password:
        return jsonify({'message': 'Usuario y contraseña son obligatorios'}), 400
    if len(password) < 6:
        return jsonify({'message': 'La contraseña debe tener al menos 6 caracteres'}), 400
    if not accepted_terms:
        return jsonify({'message': 'Debes aceptar los términos para crear la cuenta.'}), 400

    try:
        with get_db() as conn:
            conn.execute(
                'INSERT INTO usuarios (username, password, terms_accepted_at) VALUES (?, ?, ?)',
                (username, hash_password(password), now_iso()),
            )
            row = conn.execute(
                'SELECT tokens FROM usuarios WHERE username = ?', (username,)
            ).fetchone()
            balance = row['tokens'] if row else 0
            add_transaction(conn, username, 'grant', balance, balance, note='Tokens iniciales (signup)')
            conn.commit()
    except sqlite3.IntegrityError:
        return jsonify({'message': 'Usuario ya existe'}), 409
    except Exception:
        log.exception('register failed for %s', username)
        return jsonify({'message': 'Error al registrar'}), 500

    log.info('register ok: %s', username)
    return jsonify({'token': generar_token(username), 'terms_version': TERMS_VERSION}), 201


@app.route('/api/login', methods=['POST'])
def login_route():
    data = request.get_json(silent=True) or {}
    username = (data.get('username') or '').strip()
    password = data.get('password') or ''
    if not username or not password:
        return jsonify({'message': 'Credenciales incorrectas'}), 401

    user = get_user(username)
    if not user:
        return jsonify({'message': 'Credenciales incorrectas'}), 401

    ok, needs_rehash = verify_password(password, user['password'])
    if not ok:
        return jsonify({'message': 'Credenciales incorrectas'}), 401

    if needs_rehash:
        try:
            with get_db() as conn:
                conn.execute(
                    'UPDATE usuarios SET password = ? WHERE username = ?',
                    (hash_password(password), username),
                )
                conn.commit()
            log.info('rehashed legacy password for %s', username)
        except Exception:
            log.exception('rehash failed for %s', username)

    return jsonify({'token': generar_token(username)})


# --- User ---
@app.route('/api/user-data', methods=['GET'])
@require_auth
def user_data():
    u = request.user
    return jsonify({
        'username': u['username'],
        'tokens': u['tokens'],
        'rol': u['rol'],
        'terms_accepted_at': u.get('terms_accepted_at'),
    })


@app.route('/api/me/transactions', methods=['GET'])
@require_auth
def my_transactions():
    limit = min(int(request.args.get('limit', 50)), 200)
    with get_db() as conn:
        rows = conn.execute(
            'SELECT id, kind, tokens_delta, balance_after, note, stripe_session_id, created_at '
            'FROM transactions WHERE owner = ? ORDER BY id DESC LIMIT ?',
            (request.user['username'], limit),
        ).fetchall()
    return jsonify({'transactions': [dict(r) for r in rows]})


@app.route('/api/legal/terms', methods=['GET'])
def legal_terms():
    text = (
        "HokoSocial — Términos & Disclaimer (v" + TERMS_VERSION + ")\n\n"
        "1. Esta herramienta automatiza acciones en plataformas de terceros (Threads/Meta).\n"
        "2. El usuario es el único responsable de su cuenta. HokoSocial no se hace responsable\n"
        "   de bloqueos, suspensiones o limitaciones que las plataformas impongan a tu cuenta\n"
        "   como consecuencia del uso de este servicio.\n"
        "3. El uso intensivo o agresivo está desaconsejado. La herramienta intenta actuar de\n"
        "   forma conservadora, pero las plataformas cambian sus reglas constantemente.\n"
        "4. Los tokens adquiridos no son reembolsables salvo error técnico imputable al servicio.\n"
        "5. No almacenamos credenciales de Threads/Meta. Las acciones se ejecutan únicamente\n"
        "   con el consentimiento explícito del usuario y a través del canal autorizado por\n"
        "   este (worker local o, en futuras versiones, importación de cookies).\n"
        "6. Al crear una cuenta confirmas haber leído y aceptado estos términos."
    )
    return jsonify({'version': TERMS_VERSION, 'text': text})


# --- Catálogo y compras ---
@app.route('/api/packs', methods=['GET'])
def list_packs():
    with get_db() as conn:
        rows = conn.execute(
            'SELECT slug, name, description, tokens, price_cents, currency '
            'FROM token_packs WHERE is_active = 1 ORDER BY tokens ASC'
        ).fetchall()
    return jsonify({
        'packs': [dict(r) for r in rows],
        'stripe_enabled': STRIPE_ENABLED,
    })


@app.route('/api/checkout', methods=['POST'])
@require_auth
def create_checkout():
    if not STRIPE_ENABLED:
        return jsonify({'message': 'Pagos no habilitados todavía. Contacta con el admin.'}), 503

    data = request.get_json(silent=True) or {}
    slug = (data.get('slug') or '').strip()
    if not slug:
        return jsonify({'message': 'Falta slug del paquete.'}), 400

    with get_db() as conn:
        pack = conn.execute(
            'SELECT slug, name, description, tokens, price_cents, currency '
            'FROM token_packs WHERE slug = ? AND is_active = 1',
            (slug,),
        ).fetchone()
    if not pack:
        return jsonify({'message': 'Paquete no encontrado.'}), 404

    success_url = f'{FRONTEND_URL}/account?status=success&session_id={{CHECKOUT_SESSION_ID}}'
    cancel_url = f'{FRONTEND_URL}/account?status=cancelled'

    try:
        session = stripe_lib.checkout.Session.create(
            mode='payment',
            line_items=[{
                'quantity': 1,
                'price_data': {
                    'currency': pack['currency'],
                    'unit_amount': pack['price_cents'],
                    'product_data': {
                        'name': f'HokoSocial — {pack["name"]} ({pack["tokens"]} tokens)',
                        'description': pack['description'] or '',
                    },
                },
            }],
            success_url=success_url,
            cancel_url=cancel_url,
            client_reference_id=request.user['username'],
            metadata={
                'username': request.user['username'],
                'pack_slug': pack['slug'],
                'tokens': str(pack['tokens']),
            },
        )
    except Exception as e:
        log.exception('stripe checkout failed: %s', e)
        return jsonify({'message': 'No pudimos iniciar el pago. Inténtalo en unos minutos.'}), 502

    return jsonify({'url': session.url, 'id': session.id})


@app.route('/api/stripe/webhook', methods=['POST'])
def stripe_webhook():
    if not STRIPE_ENABLED:
        return jsonify({'message': 'Stripe no configurado'}), 503

    payload = request.get_data(as_text=False)
    sig_header = request.headers.get('Stripe-Signature', '')

    try:
        if STRIPE_WEBHOOK_SECRET:
            event = stripe_lib.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
        else:
            # En dev sin webhook secret, parseamos confiando en la firma TLS de Stripe.
            log.warning('STRIPE_WEBHOOK_SECRET no definido — verificación de firma desactivada.')
            event = json.loads(payload)
    except (ValueError, getattr(stripe_lib, 'error', type('E', (), {})).SignatureVerificationError if stripe_lib else ValueError) as e:
        log.warning('webhook firma inválida: %s', e)
        return jsonify({'message': 'invalid signature'}), 400

    etype = event.get('type') if isinstance(event, dict) else event['type']
    data_obj = event['data']['object'] if isinstance(event, dict) else event.data.object

    if etype == 'checkout.session.completed':
        session_id = data_obj.get('id') if isinstance(data_obj, dict) else data_obj.id
        meta = data_obj.get('metadata') if isinstance(data_obj, dict) else (data_obj.metadata or {})
        username = (meta or {}).get('username')
        tokens = int((meta or {}).get('tokens') or 0)
        pack_slug = (meta or {}).get('pack_slug', '?')
        if username and tokens > 0:
            credit_tokens(
                username, tokens, 'purchase',
                note=f'Compra de paquete {pack_slug}',
                stripe_session_id=session_id,
            )
        else:
            log.warning('checkout.session.completed sin metadata útil: %s', meta)

    return jsonify({'received': True})


# --- Helpers de tareas ---
def task_to_dict(row, include_log=True):
    d = dict(row)
    if include_log:
        log_text = d.pop('log', '') or ''
        d['log_lines'] = [l for l in log_text.split('\n') if l]
    else:
        d.pop('log', None)
    # payload / result son JSON, los devolvemos parseados al cliente.
    raw_payload = d.pop('payload', None)
    if raw_payload:
        try:
            d['payload'] = json.loads(raw_payload)
        except Exception:
            d['payload'] = None
    else:
        d['payload'] = None
    raw_result = d.pop('result', None)
    if raw_result:
        try:
            d['result'] = json.loads(raw_result)
        except Exception:
            d['result'] = None
    else:
        d['result'] = None
    return d


def append_log_lines(task_id: int, lines):
    if not lines:
        return
    cleaned = [str(l).rstrip() for l in lines if str(l).strip()]
    if not cleaned:
        return
    with get_db() as conn:
        row = conn.execute('SELECT log FROM tasks WHERE id = ?', (task_id,)).fetchone()
        if not row:
            return
        existing = (row['log'] or '').split('\n') if row['log'] else []
        merged = (existing + cleaned)[-MAX_LOG_LINES:]
        conn.execute('UPDATE tasks SET log = ? WHERE id = ?', ('\n'.join(merged), task_id))
        conn.commit()


def run_simulation(task_id: int):
    try:
        append_log_lines(task_id, ['🔄 Ejecutando bot (simulación)...'])
        for i in range(3):
            time.sleep(1)
            append_log_lines(task_id, [f'⏳ Proceso {i+1}/3'])
        append_log_lines(task_id, ['✅ Bot finalizado'])
        with get_db() as conn:
            conn.execute(
                "UPDATE tasks SET status='done', finished_at=? WHERE id=?",
                (now_iso(), task_id),
            )
            conn.commit()
    except Exception as e:
        log.exception('simulation failed for task %s', task_id)
        with get_db() as conn:
            conn.execute(
                "UPDATE tasks SET status='failed', error=?, finished_at=? WHERE id=?",
                (str(e), now_iso(), task_id),
            )
            conn.commit()


def run_footprint_scan(task_id: int, owner: str, payload: dict):
    """Corre digital_footprint.run_full_scan en un thread y persiste el resultado."""
    from digital_footprint import run_full_scan

    def push(line: str):
        append_log_lines(task_id, [line])

    try:
        result = run_full_scan(
            email=payload.get('email'),
            phone=payload.get('phone'),
            image_data_b64=payload.get('image_b64'),
            log_fn=push,
        )
    except ValueError as e:
        push(f'❌ {e}')
        with get_db() as conn:
            conn.execute(
                "UPDATE tasks SET status='failed', error=?, finished_at=? WHERE id=?",
                (str(e), now_iso(), task_id),
            )
            conn.commit()
        return
    except Exception as e:
        log.exception('footprint scan failed for task %s', task_id)
        push(f'❌ Error interno: {e}')
        with get_db() as conn:
            conn.execute(
                "UPDATE tasks SET status='failed', error=?, finished_at=? WHERE id=?",
                (str(e)[:500], now_iso(), task_id),
            )
            conn.commit()
        return

    # Persistir en footprint_scans + actualizar tasks.result
    serialized = json.dumps(result)
    with get_db() as conn:
        conn.execute(
            'INSERT INTO footprint_scans '
            '(owner, created_at, email_hash, phone_hash, email_masked, phone_masked, '
            ' face_used, score, result_json, task_id) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (
                owner, now_iso(),
                result.get('email_hash'), result.get('phone_hash'),
                result.get('email_masked'), result.get('phone_masked'),
                1 if result.get('face_used') else 0,
                int(result.get('score') or 0),
                serialized, task_id,
            ),
        )
        conn.execute(
            "UPDATE tasks SET result=?, status='done', finished_at=? WHERE id=?",
            (serialized, now_iso(), task_id),
        )
        conn.commit()
    push(f'🎉 Scan completado. Score de exposición: {result.get("score")}/100')


# --- Bot endpoints de usuario ---
@app.route('/api/tasks', methods=['POST'])
@require_auth
def create_task():
    data = request.get_json(silent=True) or {}
    task_type = (data.get('type') or 'simulate').strip()
    payload = data.get('payload')
    if task_type not in TASK_TYPES:
        return jsonify({'message': f"type debe ser uno de: {', '.join(TASK_TYPES)}"}), 400

    # Validaciones por tipo
    if task_type == 'instagram_profile':
        action = (payload or {}).get('action') or 'profile'
        if action == 'profile':
            ig_user = ((payload or {}).get('username') or '').strip().lstrip('@')
            if not ig_user or not ig_user.replace('.', '').replace('_', '').isalnum():
                return jsonify({'message': 'username de Instagram inválido.'}), 400
            payload = {'action': 'profile', 'username': ig_user}
        elif action in ('feed', 'stories', 'reels'):
            user_id = str((payload or {}).get('user_id') or '').strip()
            if not user_id.isdigit():
                return jsonify({'message': f'user_id inválido para action={action}.'}), 400
            normalized = {'action': action, 'user_id': user_id}
            if action in ('feed', 'reels') and (payload or {}).get('max_id'):
                normalized['max_id'] = str(payload['max_id'])
            payload = normalized
        elif action in ('highlights', 'tagged'):
            ig_user = ((payload or {}).get('username') or '').strip().lstrip('@')
            if not ig_user:
                return jsonify({'message': 'username inválido.'}), 400
            payload = {'action': action, 'username': ig_user}
        else:
            return jsonify({'message': f'action no soportada: {action}'}), 400
    elif task_type == 'ig_snapshot':
        ig_user = ((payload or {}).get('username') or '').strip().lstrip('@')
        if ig_user and not ig_user.replace('.', '').replace('_', '').isalnum():
            return jsonify({'message': 'username de Instagram inválido.'}), 400
        payload = {'username': ig_user} if ig_user else {}
    elif task_type == 'ig_growth_view_stories':
        p = payload or {}
        candidate_ids = p.get('candidate_ids') or []
        if not isinstance(candidate_ids, list) or not candidate_ids:
            return jsonify({'message': 'candidate_ids requerido (lista de ints).'}), 400
        candidate_ids = [int(x) for x in candidate_ids][:20]  # cap a 20 por task
        # Cargar candidatas (verificando ownership) + IG pks para el worker
        with get_db() as conn:
            placeholders = ','.join('?' for _ in candidate_ids)
            rows = conn.execute(
                f'SELECT id, ig_pk, ig_username FROM ig_growth_candidates '
                f'WHERE owner = ? AND id IN ({placeholders})',
                [request.user['username']] + candidate_ids,
            ).fetchall()
        if not rows:
            return jsonify({'message': 'Ninguna candidata coincide.'}), 404
        payload = {
            'candidates': [{
                'candidate_id': r['id'],
                'ig_pk': r['ig_pk'],
                'ig_username': r['ig_username'],
            } for r in rows],
        }
    elif task_type == 'ig_growth_discover':
        p = payload or {}
        source_id = p.get('source_id')
        max_candidates = int(p.get('max_candidates') or 30)
        if max_candidates < 1 or max_candidates > 100:
            return jsonify({'message': 'max_candidates debe estar entre 1 y 100.'}), 400
        if source_id is None:
            return jsonify({'message': 'source_id es obligatorio.'}), 400
        with get_db() as conn:
            row = conn.execute(
                'SELECT id, kind, value, niche FROM ig_growth_sources '
                'WHERE id = ? AND owner = ?',
                (source_id, request.user['username']),
            ).fetchone()
        if not row:
            return jsonify({'message': 'source_id no encontrado.'}), 404
        # Enriquecemos el payload con source_detail para que el worker no
        # necesite hacer una segunda llamada al backend (que requeriría JWT
        # de usuario, no la worker key).
        payload = {
            'source_id': source_id,
            'source_detail': {
                'id': row['id'],
                'kind': row['kind'],
                'value': row['value'],
                'niche': row['niche'],
            },
            'max_candidates': max_candidates,
            'min_score': int(p.get('min_score') or 0),
        }
    elif task_type == 'footprint_scan':
        p = payload or {}
        email = (p.get('email') or '').strip().lower()
        phone = (p.get('phone') or '').strip()
        image_b64 = p.get('image_b64')
        consent = bool(p.get('self_consent'))
        if not consent:
            return jsonify({
                'message': 'Tienes que confirmar que estos datos son tuyos (self_consent=true).',
            }), 400
        if not (email or phone or image_b64):
            return jsonify({'message': 'Pásanos al menos un email, teléfono o foto.'}), 400
        if email and '@' not in email:
            return jsonify({'message': 'Email inválido.'}), 400
        if image_b64 and not isinstance(image_b64, str):
            return jsonify({'message': 'image_b64 debe ser string base64.'}), 400
        payload = {
            'email': email or None,
            'phone': phone or None,
            'image_b64': image_b64 or None,
            'self_consent': True,
        }

    username = request.user['username']
    cost = _task_token_cost(task_type, payload)
    ok, tokens_restantes = consume_n_tokens(username, cost, note=f'Tarea {task_type} ({cost}t)')
    if not ok:
        return jsonify({
            'message': f'Necesitas {cost} token{"s" if cost != 1 else ""} para esta tarea',
            'tokens_restantes': tokens_restantes,
            'cost': cost,
        }), 402

    payload_json = json.dumps(payload) if payload is not None else None

    with get_db() as conn:
        cur = conn.execute(
            'INSERT INTO tasks (owner, type, status, created_at, payload) VALUES (?, ?, ?, ?, ?)',
            (username, task_type, 'queued', now_iso(), payload_json),
        )
        conn.commit()
        task_id = cur.lastrowid

    log.info('queued task %s type=%s for %s (cost=%d)', task_id, task_type, username, cost)

    if task_type == 'simulate':
        with get_db() as conn:
            conn.execute(
                "UPDATE tasks SET status='running', started_at=? WHERE id=?",
                (now_iso(), task_id),
            )
            conn.commit()
        threading.Thread(target=run_simulation, args=(task_id,), daemon=True).start()
    elif task_type == 'footprint_scan':
        with get_db() as conn:
            conn.execute(
                "UPDATE tasks SET status='running', started_at=? WHERE id=?",
                (now_iso(), task_id),
            )
            conn.commit()
        threading.Thread(target=run_footprint_scan, args=(task_id, username, payload), daemon=True).start()

    immediately_running = task_type in ('simulate', 'footprint_scan')
    return jsonify({
        'id': task_id,
        'type': task_type,
        'status': 'running' if immediately_running else 'queued',
        'tokens_restantes': tokens_restantes,
        'cost': cost,
        'message': 'Tarea encolada.' if not immediately_running else 'Tarea lanzada.',
    }), 201


def _task_token_cost(task_type: str, payload: dict | None) -> int:
    """Coste en tokens. footprint_scan con foto cuesta 2 (FaceCheck consume API)."""
    if task_type == 'footprint_scan' and isinstance(payload, dict) and payload.get('image_b64'):
        return 2
    return 1


@app.route('/api/tasks/<int:task_id>', methods=['GET'])
@require_auth
def get_task(task_id):
    with get_db() as conn:
        row = conn.execute('SELECT * FROM tasks WHERE id = ?', (task_id,)).fetchone()
    if not row:
        return jsonify({'message': 'Tarea no encontrada'}), 404
    if row['owner'] != request.user['username'] and request.user.get('rol') != 'admin':
        return jsonify({'message': 'No puedes ver esa tarea'}), 403
    return jsonify(task_to_dict(row))


@app.route('/api/tasks', methods=['GET'])
@require_auth
def list_tasks():
    limit = min(int(request.args.get('limit', 20)), 100)
    with get_db() as conn:
        rows = conn.execute(
            'SELECT * FROM tasks WHERE owner = ? ORDER BY id DESC LIMIT ?',
            (request.user['username'], limit),
        ).fetchall()
    return jsonify({'tasks': [task_to_dict(r, include_log=False) for r in rows]})


@app.route('/api/run-bot', methods=['POST'])
@require_auth
def run_bot_legacy():
    """Compat: encola un simulate y devuelve el contrato anterior."""
    username = request.user['username']
    ok, tokens_restantes = consume_one_token(username, note='Tarea simulate (legacy)')
    if not ok:
        return jsonify({
            'message': 'No tienes tokens disponibles',
            'tokens_restantes': tokens_restantes,
        }), 402

    with get_db() as conn:
        cur = conn.execute(
            'INSERT INTO tasks (owner, type, status, created_at, started_at) VALUES (?, ?, ?, ?, ?)',
            (username, 'simulate', 'running', now_iso(), now_iso()),
        )
        conn.commit()
        task_id = cur.lastrowid

    threading.Thread(target=run_simulation, args=(task_id,), daemon=True).start()
    return jsonify({
        'message': 'Bot lanzado correctamente',
        'tokens_restantes': tokens_restantes,
        'task_id': task_id,
    })


@app.route('/api/log', methods=['GET'])
@require_auth
def get_log_legacy():
    username = request.user['username']
    with get_db() as conn:
        row = conn.execute(
            'SELECT * FROM tasks WHERE owner = ? ORDER BY id DESC LIMIT 1',
            (username,),
        ).fetchone()
    if not row:
        return jsonify({'log': [], 'mensaje_bot': 'Sin log aún.'})
    lines = [l for l in (row['log'] or '').split('\n') if l]
    return jsonify({
        'log': lines,
        'mensaje_bot': lines[-1] if lines else 'Sin mensaje final',
        'task_id': row['id'],
        'task_status': row['status'],
        'task_type': row['type'],
    })


@app.route('/api/generar-cookies', methods=['POST'])
@require_auth
def generar_cookies():
    return jsonify({'message': 'La generación de cookies aún no está implementada en el servidor.'}), 501


# --- Instagram (server-side, con pool de cuentas-bot) ---
@app.route('/api/instagram/profile', methods=['POST'])
@require_auth
def instagram_profile_endpoint():
    """Scrapea un perfil público de Instagram desde el servidor.

    Si Instagram bloquea la IP (datacenter), devuelve 502 con código
    `instagram_blocked` para que el frontend pueda ofrecer caer a una
    tarea procesada por el worker en PC del usuario.

    Cobra 1 token solo si la operación es exitosa (devuelve 200) o si
    el username no existe (404, no es culpa nuestra). En caso de bloqueo
    o error de red NO cobra el token.
    """
    from instagram_scraper import (
        InstagramBlockedError,
        InstagramNotFoundError,
        InstagramScrapeError,
        scrape_profile,
    )

    data = request.get_json(silent=True) or {}
    username_in = (data.get('username') or '').strip().lstrip('@')
    if not username_in or not username_in.replace('.', '').replace('_', '').isalnum():
        return jsonify({'message': 'username inválido.'}), 400

    # Comprueba saldo antes de scrapear (sin consumir todavía).
    with get_db() as conn:
        row = conn.execute(
            'SELECT tokens FROM usuarios WHERE username = ?',
            (request.user['username'],),
        ).fetchone()
    if not row or (row['tokens'] or 0) <= 0:
        return jsonify({
            'message': 'No tienes tokens disponibles.',
            'tokens_restantes': 0,
        }), 402

    # Intentamos scrapear ANTES de cobrar para no quemar token si Instagram bloquea.
    # Si hay cuentas-bot activas, usamos su cookie (libera bloqueo IP de Render).
    from instagram_scraper import InstagramAuthExpiredError
    profile = None
    last_block_error = None
    for _ in range(5):
        bot = pick_bot_account()
        cookies = bot['cookies'] if bot else None
        try:
            profile = scrape_profile(username_in, cookies=cookies)
            break
        except InstagramNotFoundError as e:
            return jsonify({'message': str(e), 'code': 'not_found'}), 404
        except InstagramAuthExpiredError as e:
            if bot:
                burn_bot_account(bot['id'], str(e))
            continue
        except InstagramBlockedError as e:
            last_block_error = str(e)
            if bot is None:
                # Sin cookies y bloqueado → caemos al worker desde el frontend.
                log.warning('IG blocked anonymous: %s', e)
                return jsonify({
                    'message': 'Instagram está bloqueando peticiones anónimas desde el servidor. '
                               'Configura una cuenta-bot o usa el worker en tu PC.',
                    'code': 'instagram_blocked',
                }), 502
            continue
        except InstagramScrapeError as e:
            log.warning('IG scrape error: %s', e)
            return jsonify({'message': str(e), 'code': 'scrape_error'}), 502
    if profile is None:
        return jsonify({
            'message': f'Pool de cuentas-bot agotado. Último error: {last_block_error}',
            'code': 'pool_exhausted',
        }), 503

    # Solo cobramos cuando la query salió bien.
    _ok, tokens_restantes = consume_one_token(request.user['username'], note=f'Instagram @{username_in}')
    return jsonify({
        'profile': profile,
        'tokens_restantes': tokens_restantes,
    })


# Tabs adicionales — cada llamada cuesta 1 token (igual que profile).
@app.route('/api/instagram/feed', methods=['POST'])
@require_auth
def instagram_feed_endpoint():
    """Feed paginado. Usa pool si hay cuentas-bot; si no, intenta anónimo."""
    from instagram_scraper import scrape_feed
    data = request.get_json(silent=True) or {}
    user_id = (data.get('user_id') or '').strip()
    if not user_id.isdigit():
        return jsonify({'message': 'user_id inválido.'}), 400
    max_id = data.get('max_id')
    return _ig_authenticated(
        scrape_feed, user_id, max_id, charge_note='Instagram feed',
        allow_anonymous_fallback=True,
    )


@app.route('/api/instagram/reels', methods=['POST'])
@require_auth
def instagram_reels_endpoint():
    """Reels: intenta con cuenta-bot del pool (libera bloqueo IP de Render).
    Si no hay pool, fallback anónimo (puede dar instagram_blocked desde cloud)."""
    from instagram_scraper import scrape_reels
    data = request.get_json(silent=True) or {}
    user_id = (data.get('user_id') or '').strip()
    if not user_id.isdigit():
        return jsonify({'message': 'user_id inválido.'}), 400
    max_id = data.get('max_id')
    return _ig_authenticated(
        scrape_reels, user_id, max_id, charge_note='Instagram reels',
        allow_anonymous_fallback=True,
    )


# --- Bot accounts pool (cuentas-bot logueadas para llamadas autenticadas) ---
BOT_REQUIRED_COOKIES = ('sessionid', 'csrftoken', 'ds_user_id')


def _normalize_cookies(raw):
    """Acepta:
    - dict {name: value}
    - lista de objetos como exporta 'EditThisCookie' / 'Cookie Editor'
      (cada item con 'name' y 'value').
    Devuelve dict.
    """
    if isinstance(raw, dict):
        return {k: v for k, v in raw.items() if v}
    if isinstance(raw, list):
        out = {}
        for item in raw:
            if isinstance(item, dict) and item.get('name'):
                out[item['name']] = item.get('value', '')
        return out
    return {}


def _validate_cookies(cookies: dict) -> str | None:
    """Devuelve mensaje de error si faltan cookies imprescindibles, sino None."""
    missing = [k for k in BOT_REQUIRED_COOKIES if not cookies.get(k)]
    if missing:
        return f'Faltan cookies obligatorias: {", ".join(missing)}'
    return None


def pick_bot_account(conn=None):
    """Devuelve la cuenta-bot active con last_used_at más antiguo (round-robin).
    Si no hay ninguna activa, devuelve None.
    Marca last_used_at antes de devolverla para minimizar carreras."""
    own_conn = conn is None
    if own_conn:
        conn = get_db()
    try:
        row = conn.execute(
            "SELECT id, username, cookies FROM bot_accounts WHERE status='active' "
            'ORDER BY (last_used_at IS NULL) DESC, last_used_at ASC LIMIT 1'
        ).fetchone()
        if not row:
            return None
        conn.execute(
            'UPDATE bot_accounts SET last_used_at = ?, used_count = used_count + 1 WHERE id = ?',
            (now_iso(), row['id']),
        )
        if own_conn:
            conn.commit()
        try:
            cookies = json.loads(row['cookies'])
        except Exception:
            cookies = {}
        return {'id': row['id'], 'username': row['username'], 'cookies': cookies}
    finally:
        if own_conn:
            conn.close()


def burn_bot_account(account_id: int, reason: str):
    with get_db() as conn:
        conn.execute(
            "UPDATE bot_accounts SET status='burned', burned_at=?, burn_reason=? WHERE id=?",
            (now_iso(), reason[:300], account_id),
        )
        conn.commit()
    log.warning('bot account #%s burned: %s', account_id, reason)


@app.route('/api/admin/bot-accounts', methods=['GET'])
@require_admin
def admin_bot_accounts_list():
    with get_db() as conn:
        rows = conn.execute(
            'SELECT id, username, status, added_at, last_used_at, used_count, '
            'burn_reason, burned_at, notes FROM bot_accounts ORDER BY id DESC'
        ).fetchall()
    return jsonify({'accounts': [dict(r) for r in rows]})


@app.route('/api/admin/bot-accounts', methods=['POST'])
@require_admin
def admin_bot_accounts_create():
    data = request.get_json(silent=True) or {}
    username = (data.get('username') or '').strip().lstrip('@')
    if not username:
        return jsonify({'message': 'username requerido'}), 400
    cookies = _normalize_cookies(data.get('cookies'))
    err = _validate_cookies(cookies)
    if err:
        return jsonify({'message': err}), 400
    notes = (data.get('notes') or '').strip()[:500] or None
    try:
        with get_db() as conn:
            conn.execute(
                'INSERT INTO bot_accounts (username, cookies, status, added_at, notes) '
                "VALUES (?, ?, 'active', ?, ?)",
                (username, json.dumps(cookies), now_iso(), notes),
            )
            conn.commit()
    except sqlite3.IntegrityError:
        return jsonify({'message': 'Ya existe una cuenta-bot con ese username'}), 409
    return jsonify({'ok': True, 'message': f'Cuenta-bot @{username} añadida'})


@app.route('/api/admin/bot-accounts/<int:bid>', methods=['DELETE'])
@require_admin
def admin_bot_accounts_delete(bid: int):
    with get_db() as conn:
        cur = conn.execute('DELETE FROM bot_accounts WHERE id = ?', (bid,))
        conn.commit()
    if cur.rowcount == 0:
        return jsonify({'message': 'No encontrada'}), 404
    return jsonify({'ok': True})


@app.route('/api/admin/bot-accounts/<int:bid>/reactivate', methods=['POST'])
@require_admin
def admin_bot_accounts_reactivate(bid: int):
    """Para volver a poner una burned como active sin re-añadir las cookies."""
    data = request.get_json(silent=True) or {}
    new_cookies = data.get('cookies')
    with get_db() as conn:
        if new_cookies is not None:
            normalized = _normalize_cookies(new_cookies)
            err = _validate_cookies(normalized)
            if err:
                return jsonify({'message': err}), 400
            conn.execute(
                "UPDATE bot_accounts SET cookies=?, status='active', burned_at=NULL, burn_reason=NULL "
                'WHERE id=?',
                (json.dumps(normalized), bid),
            )
        else:
            conn.execute(
                "UPDATE bot_accounts SET status='active', burned_at=NULL, burn_reason=NULL WHERE id=?",
                (bid,),
            )
        conn.commit()
    return jsonify({'ok': True})


# --- Endpoints autenticados de Instagram (usan pool de cuentas-bot) ---
def _ig_authenticated(scraper_fn, *args, charge_note: str, allow_anonymous_fallback: bool = False):
    """Wrapper común para los endpoints que requieren cookie de cuenta-bot.

    Comprueba saldo, escoge una cuenta-bot, ejecuta scraper. Si IG rechaza la
    cookie (ExpiredAuth), la marca burned y reintenta hasta agotar el pool.
    """
    from instagram_scraper import (
        InstagramAuthExpiredError,
        InstagramBlockedError,
        InstagramNotFoundError,
        InstagramScrapeError,
    )

    user = request.user['username']
    with get_db() as conn:
        row = conn.execute('SELECT tokens FROM usuarios WHERE username = ?', (user,)).fetchone()
    if not row or (row['tokens'] or 0) <= 0:
        return jsonify({'message': 'No tienes tokens disponibles.', 'tokens_restantes': 0}), 402

    last_error = None
    tried = 0
    while tried < 5:
        bot = pick_bot_account()
        if not bot:
            if allow_anonymous_fallback:
                # último intento sin cookie, IG puede o no responder.
                try:
                    result = scraper_fn(*args, cookies=None)
                    _ok, tokens_restantes = consume_one_token(user, note=charge_note)
                    return jsonify({'data': result, 'tokens_restantes': tokens_restantes,
                                    'used_bot': None, 'fallback_anonymous': True})
                except InstagramBlockedError as e:
                    return jsonify({
                        'message': 'No hay cuentas-bot activas y la IP del servidor está bloqueada.',
                        'code': 'no_bots_available',
                        'detail': str(e),
                    }), 503
                except InstagramNotFoundError as e:
                    return jsonify({'message': str(e), 'code': 'not_found'}), 404
                except InstagramScrapeError as e:
                    return jsonify({'message': str(e), 'code': 'scrape_error'}), 502
            return jsonify({
                'message': 'No hay cuentas-bot activas. Pide al admin que añada una.',
                'code': 'no_bots_available',
            }), 503

        try:
            result = scraper_fn(*args, cookies=bot['cookies'])
            _ok, tokens_restantes = consume_one_token(user, note=charge_note)
            return jsonify({
                'data': result,
                'tokens_restantes': tokens_restantes,
                'used_bot': bot['username'],
            })
        except InstagramAuthExpiredError as e:
            burn_bot_account(bot['id'], str(e))
            last_error = str(e)
            tried += 1
            continue
        except InstagramNotFoundError as e:
            return jsonify({'message': str(e), 'code': 'not_found'}), 404
        except InstagramBlockedError as e:
            last_error = str(e)
            tried += 1
            continue
        except InstagramScrapeError as e:
            return jsonify({'message': str(e), 'code': 'scrape_error'}), 502

    return jsonify({
        'message': f'Todas las cuentas-bot disponibles fallaron. Último error: {last_error}',
        'code': 'pool_exhausted',
    }), 503


@app.route('/api/instagram/stories', methods=['POST'])
@require_auth
def instagram_stories_endpoint_v2():
    """Reemplaza al endpoint anterior. Usa pool de cuentas-bot."""
    from instagram_scraper import scrape_stories
    data = request.get_json(silent=True) or {}
    user_id = (data.get('user_id') or '').strip()
    if not user_id.isdigit():
        return jsonify({'message': 'user_id inválido.'}), 400
    return _ig_authenticated(scrape_stories, user_id, charge_note='Instagram stories')


@app.route('/api/instagram/highlights', methods=['POST'])
@require_auth
def instagram_highlights_endpoint_v2():
    from instagram_scraper import scrape_highlights
    data = request.get_json(silent=True) or {}
    user_id = (data.get('user_id') or '').strip()
    if not user_id.isdigit():
        return jsonify({'message': 'user_id inválido.'}), 400
    return _ig_authenticated(scrape_highlights, user_id, charge_note='Instagram highlights')


@app.route('/api/instagram/followers', methods=['POST'])
@require_auth
def instagram_followers_endpoint():
    from instagram_scraper import scrape_followers
    data = request.get_json(silent=True) or {}
    user_id = (data.get('user_id') or '').strip()
    if not user_id.isdigit():
        return jsonify({'message': 'user_id inválido.'}), 400
    max_id = data.get('max_id')
    return _ig_authenticated(scrape_followers, user_id, max_id, charge_note='Instagram followers')


@app.route('/api/instagram/following', methods=['POST'])
@require_auth
def instagram_following_endpoint():
    from instagram_scraper import scrape_following
    data = request.get_json(silent=True) or {}
    user_id = (data.get('user_id') or '').strip()
    if not user_id.isdigit():
        return jsonify({'message': 'user_id inválido.'}), 400
    max_id = data.get('max_id')
    return _ig_authenticated(scrape_following, user_id, max_id, charge_note='Instagram following')


# --- Instagram analyzer (snapshots de followers/following) ---
def _by_pk(users):
    return {u.get('pk'): u for u in (users or []) if isinstance(u, dict) and u.get('pk')}


def _sort_users(users):
    return sorted(users, key=lambda u: (u.get('username') or '').lower())


def _ig_snapshot_row_to_dict(row, include_lists: bool = False):
    out = {
        'id': row['id'],
        'ig_username': row['ig_username'],
        'taken_at': row['taken_at'],
        'counts': {
            'followers': row['followers_count'],
            'following': row['following_count'],
            'mutuals': row['mutuals_count'],
            'not_following_back': row['not_following_back_count'],
            'fans': row['fans_count'],
        },
        'profile': json.loads(row['profile_json'] or '{}'),
        'task_id': row['task_id'],
    }
    if include_lists:
        out['followers'] = json.loads(row['followers_json'] or '[]')
        out['following'] = json.loads(row['following_json'] or '[]')
    return out


def _resolve_target_username(req, user) -> str | None:
    """Toma ?username=X o, si no viene, el del propio user (no aplicable aquí
    porque el snapshot es de la cuenta IG, no del user de hokosocial).

    Devuelve None si no es válido. Actualmente requiere que el frontend pase
    el username explícitamente.
    """
    raw = (req.args.get('username') or '').strip().lstrip('@').lower()
    if not raw or not raw.replace('.', '').replace('_', '').isalnum():
        return None
    return raw


@app.route('/api/instagram/snapshot/latest', methods=['GET'])
@require_auth
def ig_snapshot_latest():
    """Devuelve el último snapshot guardado del usuario para una cuenta IG.

    Query: ?username=<ig_username>&include_lists=1 (opcional, gratis).
    """
    target = _resolve_target_username(request, request.user)
    if not target:
        return jsonify({'message': 'username inválido o ausente.'}), 400
    include_lists = request.args.get('include_lists') in ('1', 'true', 'yes')
    with get_db() as conn:
        row = conn.execute(
            'SELECT * FROM ig_snapshots WHERE owner = ? AND ig_username = ? '
            'ORDER BY taken_at DESC LIMIT 1',
            (request.user['username'], target),
        ).fetchone()
    if not row:
        return jsonify({'snapshot': None}), 200
    return jsonify({'snapshot': _ig_snapshot_row_to_dict(row, include_lists=include_lists)})


@app.route('/api/instagram/snapshot/status', methods=['GET'])
@require_auth
def ig_snapshot_status():
    """Cruces calculables con UN solo snapshot: not_following_back, fans, mutuals.

    Query: ?username=<ig_username>&limit=200 (default 200, max 5000).
    """
    target = _resolve_target_username(request, request.user)
    if not target:
        return jsonify({'message': 'username inválido o ausente.'}), 400
    try:
        limit = max(1, min(int(request.args.get('limit', 200)), 5000))
    except ValueError:
        limit = 200
    with get_db() as conn:
        row = conn.execute(
            'SELECT * FROM ig_snapshots WHERE owner = ? AND ig_username = ? '
            'ORDER BY taken_at DESC LIMIT 1',
            (request.user['username'], target),
        ).fetchone()
    if not row:
        return jsonify({'status': None}), 200
    followers = json.loads(row['followers_json'] or '[]')
    following = json.loads(row['following_json'] or '[]')
    f_map = _by_pk(followers)
    g_map = _by_pk(following)
    not_back = _sort_users([g_map[pk] for pk in g_map.keys() - f_map.keys()])
    fans = _sort_users([f_map[pk] for pk in f_map.keys() - g_map.keys()])
    mutuals = _sort_users([f_map[pk] for pk in f_map.keys() & g_map.keys()])
    return jsonify({
        'status': {
            'ig_username': row['ig_username'],
            'taken_at': row['taken_at'],
            'profile': json.loads(row['profile_json'] or '{}'),
            'counts': {
                'followers': len(followers),
                'following': len(following),
                'mutuals': len(mutuals),
                'not_following_back': len(not_back),
                'fans': len(fans),
            },
            'not_following_back': not_back[:limit],
            'fans': fans[:limit],
        }
    })


@app.route('/api/instagram/snapshot/diff', methods=['GET'])
@require_auth
def ig_snapshot_diff():
    """Diff entre los DOS últimos snapshots del owner para una cuenta IG.

    Query: ?username=<ig_username>&limit=500.
    """
    target = _resolve_target_username(request, request.user)
    if not target:
        return jsonify({'message': 'username inválido o ausente.'}), 400
    try:
        limit = max(1, min(int(request.args.get('limit', 500)), 5000))
    except ValueError:
        limit = 500
    with get_db() as conn:
        rows = conn.execute(
            'SELECT * FROM ig_snapshots WHERE owner = ? AND ig_username = ? '
            'ORDER BY taken_at DESC LIMIT 2',
            (request.user['username'], target),
        ).fetchall()
    if len(rows) < 2:
        return jsonify({'diff': None, 'snapshots_available': len(rows)}), 200
    curr, prev = rows[0], rows[1]
    p_followers = _by_pk(json.loads(prev['followers_json'] or '[]'))
    c_followers = _by_pk(json.loads(curr['followers_json'] or '[]'))
    p_following = _by_pk(json.loads(prev['following_json'] or '[]'))
    c_following = _by_pk(json.loads(curr['following_json'] or '[]'))
    lost = _sort_users([c_followers.get(pk) or p_followers[pk] for pk in p_followers.keys() - c_followers.keys()])
    new_f = _sort_users([c_followers[pk] for pk in c_followers.keys() - p_followers.keys()])
    stopped = _sort_users([p_following[pk] for pk in p_following.keys() - c_following.keys()])
    started = _sort_users([c_following[pk] for pk in c_following.keys() - p_following.keys()])
    return jsonify({
        'diff': {
            'ig_username': curr['ig_username'],
            'taken_at': curr['taken_at'],
            'previous_at': prev['taken_at'],
            'follower_counts': {
                'prev': prev['followers_count'],
                'curr': curr['followers_count'],
                'delta': curr['followers_count'] - prev['followers_count'],
            },
            'following_counts': {
                'prev': prev['following_count'],
                'curr': curr['following_count'],
                'delta': curr['following_count'] - prev['following_count'],
            },
            'lost_followers': lost[:limit],
            'new_followers': new_f[:limit],
            'stopped_following': stopped[:limit],
            'started_following': started[:limit],
        }
    })


@app.route('/api/instagram/snapshot/history', methods=['GET'])
@require_auth
def ig_snapshot_history():
    """Histórico de counts (sin listas) para gráfica.

    Query: ?username=<ig_username>&limit=60 (default 60, max 365).
    """
    target = _resolve_target_username(request, request.user)
    if not target:
        return jsonify({'message': 'username inválido o ausente.'}), 400
    try:
        limit = max(1, min(int(request.args.get('limit', 60)), 365))
    except ValueError:
        limit = 60
    with get_db() as conn:
        rows = conn.execute(
            'SELECT id, taken_at, followers_count, following_count, mutuals_count, '
            'not_following_back_count, fans_count FROM ig_snapshots '
            'WHERE owner = ? AND ig_username = ? ORDER BY taken_at DESC LIMIT ?',
            (request.user['username'], target, limit),
        ).fetchall()
    items = [{
        'id': r['id'],
        'taken_at': r['taken_at'],
        'followers': r['followers_count'],
        'following': r['following_count'],
        'mutuals': r['mutuals_count'],
        'not_following_back': r['not_following_back_count'],
        'fans': r['fans_count'],
    } for r in rows]
    return jsonify({'history': list(reversed(items))})


# Image proxy — gratis, evita problemas de CORS/Origin con CDN de Instagram.
@app.route('/api/instagram/image', methods=['GET'])
def instagram_image_proxy():
    from urllib.parse import urlparse

    src = request.args.get('u', '')
    if not src.startswith('https://'):
        return jsonify({'message': 'url inválida'}), 400
    host = urlparse(src).hostname or ''
    # Solo CDNs conocidos de Instagram para evitar SSRF.
    if not (host.endswith('cdninstagram.com') or host.endswith('fbcdn.net')):
        return jsonify({'message': 'host no permitido'}), 400
    try:
        import requests as _req
        upstream = _req.get(src, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64) Chrome/127.0',
            'Referer': 'https://www.instagram.com/',
        }, timeout=10, stream=True)
    except Exception as e:
        return jsonify({'message': f'fetch error: {e}'}), 502
    if upstream.status_code != 200:
        return jsonify({'message': f'upstream {upstream.status_code}'}), 502
    from flask import Response
    content_type = upstream.headers.get('Content-Type', 'image/jpeg')
    return Response(
        upstream.iter_content(chunk_size=8192),
        status=200,
        content_type=content_type,
        headers={'Cache-Control': 'public, max-age=3600'},
    )


# --- Worker — heartbeat y cola ---
@app.route('/api/worker/heartbeat', methods=['POST'])
@require_worker
def worker_heartbeat():
    with get_db() as conn:
        conn.execute('UPDATE worker_status SET last_seen = ? WHERE id = 1', (now_iso(),))
        conn.commit()
    return jsonify({'ok': True, 'server_time': now_iso()})


def cleanup_zombie_tasks(conn) -> int:
    """Marca como 'failed' las tareas que llevan >ZOMBIE_TASK_AFTER segundos
    en 'running' y resetea worker_status.current_task_id si apuntaba a una."""
    cutoff = (datetime.datetime.utcnow() - datetime.timedelta(seconds=ZOMBIE_TASK_AFTER)
              ).isoformat(timespec='seconds') + 'Z'
    cur = conn.execute(
        "UPDATE tasks SET status='failed', finished_at=?, "
        "error='Tarea zombie: el worker no reportó progreso en ' || ? || 's. Auto-cancelada.' "
        "WHERE status='running' AND started_at IS NOT NULL AND started_at < ?",
        (now_iso(), ZOMBIE_TASK_AFTER, cutoff),
    )
    if cur.rowcount > 0:
        log.info('cleanup_zombie_tasks: %d tareas marcadas como failed', cur.rowcount)
        conn.execute(
            'UPDATE worker_status SET current_task_id = NULL WHERE id = 1 AND '
            'current_task_id NOT IN (SELECT id FROM tasks WHERE status = "running")',
        )
    return cur.rowcount


@app.route('/api/worker/next', methods=['POST'])
@require_worker
def worker_next():
    placeholders = ','.join('?' for _ in WORKER_TYPES)
    with get_db() as conn:
        # Limpia tareas zombie antes de coger una nueva — protege contra
        # workers que crashearon sin reportar finish.
        cleanup_zombie_tasks(conn)
        row = conn.execute(
            f"SELECT id FROM tasks WHERE status='queued' AND type IN ({placeholders}) "
            "ORDER BY id ASC LIMIT 1",
            WORKER_TYPES,
        ).fetchone()
        if not row:
            conn.execute(
                'UPDATE worker_status SET last_seen = ?, current_task_id = NULL WHERE id = 1',
                (now_iso(),),
            )
            conn.commit()
            return jsonify({'task': None})
        task_id = row['id']
        cur = conn.execute(
            "UPDATE tasks SET status='running', started_at=? WHERE id=? AND status='queued'",
            (now_iso(), task_id),
        )
        if cur.rowcount == 0:
            return jsonify({'task': None})
        conn.execute(
            'UPDATE worker_status SET last_seen = ?, current_task_id = ? WHERE id = 1',
            (now_iso(), task_id),
        )
        conn.commit()
        task = conn.execute('SELECT * FROM tasks WHERE id = ?', (task_id,)).fetchone()
    return jsonify({'task': task_to_dict(task, include_log=False)})


@app.route('/api/worker/<int:task_id>/log', methods=['POST'])
@require_worker
def worker_log(task_id):
    data = request.get_json(silent=True) or {}
    lines = data.get('lines')
    if lines is None and 'line' in data:
        lines = [data.get('line')]
    if not isinstance(lines, list):
        return jsonify({'message': 'Body inválido. Usa {lines: [...]} o {line: "..."}'}), 400
    append_log_lines(task_id, lines)
    with get_db() as conn:
        conn.execute('UPDATE worker_status SET last_seen = ? WHERE id = 1', (now_iso(),))
        conn.commit()
    return jsonify({'ok': True})


@app.route('/api/worker/<int:task_id>/result', methods=['POST'])
@require_worker
def worker_result(task_id):
    """El worker guarda un dict de resultado estructurado (ej. perfil Instagram).

    Si la tarea es `ig_snapshot`, además persiste el snapshot completo en la
    tabla `ig_snapshots` para queries históricas (último estado, diff, etc.).
    """
    data = request.get_json(silent=True) or {}
    result = data.get('result')
    if result is None:
        return jsonify({'message': 'Falta result.'}), 400
    try:
        serialized = json.dumps(result)
    except Exception:
        return jsonify({'message': 'result no serializable a JSON'}), 400
    with get_db() as conn:
        task_row = conn.execute('SELECT type, owner FROM tasks WHERE id = ?', (task_id,)).fetchone()
        if not task_row:
            return jsonify({'message': 'Tarea no encontrada'}), 404
        cur = conn.execute('UPDATE tasks SET result = ? WHERE id = ?', (serialized, task_id))
        conn.execute('UPDATE worker_status SET last_seen = ? WHERE id = 1', (now_iso(),))
        if task_row['type'] == 'ig_snapshot' and isinstance(result, dict):
            try:
                _persist_ig_snapshot(conn, task_id, task_row['owner'], result)
            except Exception:
                log.exception('No se pudo persistir ig_snapshot del task %s', task_id)
        conn.commit()
    if cur.rowcount == 0:
        return jsonify({'message': 'Tarea no encontrada'}), 404
    return jsonify({'ok': True})


def _persist_ig_snapshot(conn, task_id: int, owner: str, result: dict) -> None:
    """Guarda un snapshot de followers/following en la tabla ig_snapshots."""
    ig_user = (result.get('username') or '').strip().lstrip('@').lower()
    if not ig_user:
        return
    followers = result.get('followers') or []
    following = result.get('following') or []
    if not isinstance(followers, list) or not isinstance(following, list):
        return
    f_pks = {u.get('pk') for u in followers if isinstance(u, dict) and u.get('pk')}
    g_pks = {u.get('pk') for u in following if isinstance(u, dict) and u.get('pk')}
    mutuals = f_pks & g_pks
    not_back = g_pks - f_pks
    fans = f_pks - g_pks
    conn.execute(
        'INSERT INTO ig_snapshots (owner, ig_username, taken_at, '
        'followers_count, following_count, mutuals_count, '
        'not_following_back_count, fans_count, '
        'profile_json, followers_json, following_json, task_id) '
        'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        (
            owner, ig_user, result.get('taken_at') or now_iso(),
            len(followers), len(following), len(mutuals), len(not_back), len(fans),
            json.dumps(result.get('profile') or {}),
            json.dumps(followers),
            json.dumps(following),
            task_id,
        ),
    )


@app.route('/api/worker/<int:task_id>/finish', methods=['POST'])
@require_worker
def worker_finish(task_id):
    data = request.get_json(silent=True) or {}
    status = (data.get('status') or 'done').strip()
    if status not in ('done', 'failed'):
        return jsonify({'message': "status debe ser 'done' o 'failed'"}), 400
    error = data.get('error')
    with get_db() as conn:
        cur = conn.execute(
            'UPDATE tasks SET status=?, finished_at=?, error=? WHERE id=? AND status="running"',
            (status, now_iso(), error, task_id),
        )
        conn.execute(
            'UPDATE worker_status SET last_seen = ?, current_task_id = NULL WHERE id = 1',
            (now_iso(),),
        )
        conn.commit()
    if cur.rowcount == 0:
        return jsonify({'message': 'La tarea no estaba en running'}), 409
    log.info('task %s finished status=%s', task_id, status)
    return jsonify({'ok': True})


@app.route('/api/worker/status', methods=['GET'])
@require_auth
def worker_status_endpoint():
    with get_db() as conn:
        # Auto-cleanup en cada poll (barato): si el worker está marcado como
        # ocupado pero la tarea es zombie, la failamos para destrabar.
        cleanup_zombie_tasks(conn)
        conn.commit()
        row = conn.execute('SELECT last_seen, current_task_id FROM worker_status WHERE id = 1').fetchone()
    if not row or not row['last_seen']:
        return jsonify({'connected': False, 'last_seen': None, 'current_task_id': None})
    last_seen_str = row['last_seen']
    try:
        last_seen = datetime.datetime.fromisoformat(last_seen_str.rstrip('Z'))
        delta = (datetime.datetime.utcnow() - last_seen).total_seconds()
        connected = delta <= WORKER_OFFLINE_AFTER
    except Exception:
        connected = False
    return jsonify({
        'connected': connected,
        'last_seen': last_seen_str,
        'current_task_id': row['current_task_id'],
    })


# --- Instagram Growth (discovery + scoring) ---
NICHE_KEYWORDS = {
    'fitness': ['gym', 'fit', 'fitness', 'workout', 'pesas', 'gimnasio', 'pump',
                'body', 'muscle', 'protein', 'training', 'crossfit', 'yoga',
                'pilates', 'runner', 'cardio', 'fitspo', 'gainz'],
    'gaming': ['gamer', 'gaming', 'twitch', 'stream', 'streamer', 'valorant',
               'fortnite', 'lol', 'csgo', 'overwatch', 'minecraft', 'nintendo',
               'playstation', 'xbox', 'pc gaming', 'esports'],
    'anime': ['anime', 'manga', 'otaku', 'weeb', 'cosplay', 'cosplayer',
              'kawaii', 'japan', 'japon', 'kpop', 'jpop', 'waifu',
              'genshin', 'honkai'],
    'moto': ['moto', 'motorbike', 'biker', 'ducati', 'bmw', 'harley',
             'kawasaki', 'yamaha', 'honda', 'suzuki', 'racing', 'motogp',
             'cbr', 'triumph', 'enduro', 'tracker', 'cafe racer', 'motera'],
}


def _ig_growth_source_to_dict(row):
    return {
        'id': row['id'],
        'kind': row['kind'],
        'value': row['value'],
        'niche': row['niche'],
        'priority': row['priority'],
        'enabled': bool(row['enabled']),
        'last_run_at': row['last_run_at'],
        'candidates_found': row['candidates_found'],
        'created_at': row['created_at'],
    }


def _ig_growth_candidate_to_dict(row):
    breakdown = None
    if row['score_breakdown']:
        try:
            breakdown = json.loads(row['score_breakdown'])
        except Exception:
            breakdown = None
    return {
        'id': row['id'],
        'ig_pk': row['ig_pk'],
        'ig_username': row['ig_username'],
        'full_name': row['full_name'],
        'profile_pic_url': row['profile_pic_url'],
        'biography': row['biography'],
        'followers_count': row['followers_count'],
        'following_count': row['following_count'],
        'posts_count': row['posts_count'],
        'is_private': bool(row['is_private']),
        'is_verified': bool(row['is_verified']),
        'score': row['score'],
        'score_breakdown': breakdown,
        'status': row['status'],
        'source_kind': row['source_kind'],
        'source_value': row['source_value'],
        'niche': row['niche'],
        'discovered_at': row['discovered_at'],
        'last_action_at': row['last_action_at'],
    }


@app.route('/api/ig/growth/sources', methods=['GET'])
@require_auth
def ig_growth_list_sources():
    with get_db() as conn:
        rows = conn.execute(
            'SELECT * FROM ig_growth_sources WHERE owner = ? '
            'ORDER BY enabled DESC, priority ASC, created_at DESC',
            (request.user['username'],),
        ).fetchall()
    return jsonify({'sources': [_ig_growth_source_to_dict(r) for r in rows]})


@app.route('/api/ig/growth/sources', methods=['POST'])
@require_auth
def ig_growth_add_source():
    data = request.get_json(silent=True) or {}
    kind = (data.get('kind') or '').strip().lower()
    value = (data.get('value') or '').strip()
    if kind not in ('hashtag', 'competitor', 'post'):
        return jsonify({'message': "kind debe ser 'hashtag', 'competitor' o 'post'."}), 400
    if kind == 'hashtag':
        value = value.lstrip('#').lower()
        if not value or not value.replace('_', '').isalnum():
            return jsonify({'message': 'hashtag inválido.'}), 400
    elif kind == 'competitor':
        value = value.lstrip('@').lower()
        if not value or not value.replace('.', '').replace('_', '').isalnum():
            return jsonify({'message': 'username inválido.'}), 400
    else:  # post
        if not value or len(value) > 32:
            return jsonify({'message': 'shortcode inválido.'}), 400
    niche = (data.get('niche') or '').strip().lower() or None
    if niche and niche not in NICHE_KEYWORDS and niche != 'mixed':
        return jsonify({'message': f"niche debe ser uno de: {', '.join(list(NICHE_KEYWORDS) + ['mixed'])}"}), 400
    priority = int(data.get('priority') or 5)
    if priority < 1 or priority > 10:
        return jsonify({'message': 'priority entre 1 y 10.'}), 400
    enabled = 1 if data.get('enabled', True) else 0
    with get_db() as conn:
        try:
            cur = conn.execute(
                'INSERT INTO ig_growth_sources '
                '(owner, kind, value, niche, priority, enabled, created_at) '
                'VALUES (?, ?, ?, ?, ?, ?, ?)',
                (request.user['username'], kind, value, niche, priority, enabled, now_iso()),
            )
            conn.commit()
            sid = cur.lastrowid
        except sqlite3.IntegrityError:
            return jsonify({'message': 'Esta fuente ya existe.'}), 409
        row = conn.execute('SELECT * FROM ig_growth_sources WHERE id = ?', (sid,)).fetchone()
    return jsonify({'source': _ig_growth_source_to_dict(row)}), 201


@app.route('/api/ig/growth/sources/<int:sid>', methods=['DELETE'])
@require_auth
def ig_growth_delete_source(sid):
    with get_db() as conn:
        cur = conn.execute(
            'DELETE FROM ig_growth_sources WHERE id = ? AND owner = ?',
            (sid, request.user['username']),
        )
        conn.commit()
    if cur.rowcount == 0:
        return jsonify({'message': 'No encontrada.'}), 404
    return jsonify({'ok': True})


@app.route('/api/ig/growth/sources/<int:sid>', methods=['PATCH'])
@require_auth
def ig_growth_patch_source(sid):
    data = request.get_json(silent=True) or {}
    fields = []
    values = []
    if 'enabled' in data:
        fields.append('enabled = ?')
        values.append(1 if data['enabled'] else 0)
    if 'priority' in data:
        p = int(data['priority'])
        if p < 1 or p > 10:
            return jsonify({'message': 'priority entre 1 y 10.'}), 400
        fields.append('priority = ?')
        values.append(p)
    if 'niche' in data:
        n = (data['niche'] or '').strip().lower() or None
        if n and n not in NICHE_KEYWORDS and n != 'mixed':
            return jsonify({'message': 'niche inválido.'}), 400
        fields.append('niche = ?')
        values.append(n)
    if not fields:
        return jsonify({'message': 'Nada que actualizar.'}), 400
    values.extend([sid, request.user['username']])
    with get_db() as conn:
        cur = conn.execute(
            f'UPDATE ig_growth_sources SET {", ".join(fields)} '
            'WHERE id = ? AND owner = ?',
            values,
        )
        conn.commit()
    if cur.rowcount == 0:
        return jsonify({'message': 'No encontrada.'}), 404
    return jsonify({'ok': True})


@app.route('/api/ig/growth/candidates', methods=['GET'])
@require_auth
def ig_growth_list_candidates():
    status = (request.args.get('status') or 'pending').strip()
    try:
        min_score = max(0, min(int(request.args.get('min_score', 0)), 100))
        limit = max(1, min(int(request.args.get('limit', 100)), 500))
    except ValueError:
        return jsonify({'message': 'min_score y limit deben ser números.'}), 400
    where = ['owner = ?', 'score >= ?']
    args = [request.user['username'], min_score]
    if status != 'all':
        where.append('status = ?')
        args.append(status)
    args.append(limit)
    with get_db() as conn:
        rows = conn.execute(
            f'SELECT * FROM ig_growth_candidates WHERE {" AND ".join(where)} '
            'ORDER BY score DESC, discovered_at DESC LIMIT ?',
            args,
        ).fetchall()
    return jsonify({'candidates': [_ig_growth_candidate_to_dict(r) for r in rows]})


@app.route('/api/ig/growth/candidates/<int:cid>', methods=['PATCH'])
@require_auth
def ig_growth_patch_candidate(cid):
    data = request.get_json(silent=True) or {}
    new_status = (data.get('status') or '').strip()
    if new_status not in ('pending', 'engaged', 'followed', 'unfollowed',
                          'converted', 'skipped', 'excluded'):
        return jsonify({'message': 'status inválido.'}), 400
    with get_db() as conn:
        cur = conn.execute(
            'UPDATE ig_growth_candidates SET status = ?, last_action_at = ? '
            'WHERE id = ? AND owner = ?',
            (new_status, now_iso(), cid, request.user['username']),
        )
        conn.commit()
    if cur.rowcount == 0:
        return jsonify({'message': 'No encontrada.'}), 404
    return jsonify({'ok': True})


@app.route('/api/ig/growth/candidates/bulk-upsert', methods=['POST'])
@require_worker
def ig_growth_candidates_bulk_upsert():
    """Endpoint que llama el worker tras descubrir candidatas — hace upsert.

    Body: {owner, source_id, candidates: [...]}.
    Cada candidate: {ig_pk, ig_username, full_name, profile_pic_url, biography,
                     followers_count, following_count, posts_count,
                     is_private, is_verified, score, score_breakdown, niche}
    """
    data = request.get_json(silent=True) or {}
    owner = (data.get('owner') or '').strip()
    source_id = data.get('source_id')
    candidates = data.get('candidates') or []
    if not owner or not isinstance(candidates, list):
        return jsonify({'message': 'Body inválido.'}), 400
    inserted = 0
    updated = 0
    with get_db() as conn:
        src = None
        if source_id:
            src = conn.execute(
                'SELECT kind, value, niche FROM ig_growth_sources WHERE id = ?',
                (source_id,),
            ).fetchone()
        for c in candidates:
            ig_pk = str(c.get('ig_pk') or '')
            if not ig_pk:
                continue
            existing = conn.execute(
                'SELECT id FROM ig_growth_candidates WHERE owner = ? AND ig_pk = ?',
                (owner, ig_pk),
            ).fetchone()
            breakdown_json = json.dumps(c.get('score_breakdown') or {})
            if existing:
                conn.execute(
                    'UPDATE ig_growth_candidates SET '
                    'ig_username = ?, full_name = ?, profile_pic_url = ?, biography = ?, '
                    'followers_count = ?, following_count = ?, posts_count = ?, '
                    'is_private = ?, is_verified = ?, score = ?, score_breakdown = ?, '
                    'niche = COALESCE(niche, ?) '
                    'WHERE id = ?',
                    (
                        c.get('ig_username'), c.get('full_name'), c.get('profile_pic_url'),
                        c.get('biography'),
                        c.get('followers_count'), c.get('following_count'), c.get('posts_count'),
                        1 if c.get('is_private') else 0,
                        1 if c.get('is_verified') else 0,
                        int(c.get('score') or 0), breakdown_json,
                        c.get('niche'),
                        existing['id'],
                    ),
                )
                updated += 1
            else:
                conn.execute(
                    'INSERT INTO ig_growth_candidates '
                    '(owner, ig_pk, ig_username, full_name, profile_pic_url, biography, '
                    ' followers_count, following_count, posts_count, '
                    ' is_private, is_verified, score, score_breakdown, '
                    ' source_id, source_kind, source_value, niche, discovered_at) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (
                        owner, ig_pk, c.get('ig_username'),
                        c.get('full_name'), c.get('profile_pic_url'), c.get('biography'),
                        c.get('followers_count'), c.get('following_count'), c.get('posts_count'),
                        1 if c.get('is_private') else 0,
                        1 if c.get('is_verified') else 0,
                        int(c.get('score') or 0), breakdown_json,
                        source_id,
                        src['kind'] if src else None,
                        src['value'] if src else None,
                        c.get('niche') or (src['niche'] if src else None),
                        now_iso(),
                    ),
                )
                inserted += 1
        if source_id:
            conn.execute(
                'UPDATE ig_growth_sources SET last_run_at = ?, '
                'candidates_found = candidates_found + ? '
                'WHERE id = ?',
                (now_iso(), inserted, source_id),
            )
        conn.commit()
    return jsonify({'ok': True, 'inserted': inserted, 'updated': updated})


@app.route('/api/ig/growth/niche-keywords', methods=['GET'])
@require_auth
def ig_growth_niche_keywords():
    return jsonify({'niches': NICHE_KEYWORDS})


# --- Growth automático (orquestador) ---------------------------------

DEFAULT_SETTINGS = {
    'auto_enabled': 0,
    'daily_view_stories_limit': 50,
    'daily_follow_limit': 30,
    'daily_like_limit': 60,
    'discovery_interval_minutes': 180,
    'engagement_interval_minutes': 25,
    'min_score': 60,
    'active_hours_start': 8,
    'active_hours_end': 23,
}


def _ensure_settings_row(conn, owner: str) -> dict:
    row = conn.execute('SELECT * FROM ig_growth_settings WHERE owner = ?', (owner,)).fetchone()
    if row:
        return dict(row)
    conn.execute(
        'INSERT INTO ig_growth_settings '
        '(owner, auto_enabled, daily_view_stories_limit, daily_follow_limit, '
        ' daily_like_limit, discovery_interval_minutes, engagement_interval_minutes, '
        ' min_score, active_hours_start, active_hours_end, updated_at) '
        'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        (owner, *[DEFAULT_SETTINGS[k] for k in (
            'auto_enabled', 'daily_view_stories_limit', 'daily_follow_limit',
            'daily_like_limit', 'discovery_interval_minutes', 'engagement_interval_minutes',
            'min_score', 'active_hours_start', 'active_hours_end',
        )], now_iso()),
    )
    conn.commit()
    return dict(conn.execute('SELECT * FROM ig_growth_settings WHERE owner = ?', (owner,)).fetchone())


def _settings_to_dict(row: dict) -> dict:
    return {
        'auto_enabled': bool(row.get('auto_enabled')),
        'daily_view_stories_limit': row.get('daily_view_stories_limit'),
        'daily_follow_limit': row.get('daily_follow_limit'),
        'daily_like_limit': row.get('daily_like_limit'),
        'discovery_interval_minutes': row.get('discovery_interval_minutes'),
        'engagement_interval_minutes': row.get('engagement_interval_minutes'),
        'min_score': row.get('min_score'),
        'active_hours_start': row.get('active_hours_start'),
        'active_hours_end': row.get('active_hours_end'),
        'last_discovery_at': row.get('last_discovery_at'),
        'last_engagement_at': row.get('last_engagement_at'),
        'paused_until': row.get('paused_until'),
        'paused_reason': row.get('paused_reason'),
    }


@app.route('/api/ig/growth/settings', methods=['GET'])
@require_auth
def ig_growth_get_settings():
    with get_db() as conn:
        row = _ensure_settings_row(conn, request.user['username'])
    return jsonify({'settings': _settings_to_dict(row)})


@app.route('/api/ig/growth/settings', methods=['PATCH'])
@require_auth
def ig_growth_patch_settings():
    data = request.get_json(silent=True) or {}
    allowed = {
        'auto_enabled': lambda v: 1 if v else 0,
        'daily_view_stories_limit': lambda v: max(1, min(int(v), 200)),
        'daily_follow_limit': lambda v: max(0, min(int(v), 150)),
        'daily_like_limit': lambda v: max(0, min(int(v), 300)),
        'discovery_interval_minutes': lambda v: max(30, min(int(v), 1440)),
        'engagement_interval_minutes': lambda v: max(5, min(int(v), 240)),
        'min_score': lambda v: max(0, min(int(v), 100)),
        'active_hours_start': lambda v: max(0, min(int(v), 23)),
        'active_hours_end': lambda v: max(1, min(int(v), 24)),
    }
    fields = []
    values = []
    for key, conv in allowed.items():
        if key in data:
            try:
                fields.append(f'{key} = ?')
                values.append(conv(data[key]))
            except (TypeError, ValueError):
                return jsonify({'message': f'{key} inválido.'}), 400
    if not fields:
        return jsonify({'message': 'Nada que actualizar.'}), 400
    fields.append('updated_at = ?')
    values.append(now_iso())
    with get_db() as conn:
        _ensure_settings_row(conn, request.user['username'])
        values.append(request.user['username'])
        conn.execute(
            f'UPDATE ig_growth_settings SET {", ".join(fields)} WHERE owner = ?',
            values,
        )
        # Si activamos auto y había paused_until, la limpiamos
        if data.get('auto_enabled') is True:
            conn.execute(
                'UPDATE ig_growth_settings SET paused_until = NULL, paused_reason = NULL '
                'WHERE owner = ?',
                (request.user['username'],),
            )
        conn.commit()
        row = conn.execute(
            'SELECT * FROM ig_growth_settings WHERE owner = ?',
            (request.user['username'],),
        ).fetchone()
    return jsonify({'settings': _settings_to_dict(dict(row))})


def _today_iso_start() -> str:
    """Devuelve YYYY-MM-DDT00:00:00Z (UTC)."""
    today = datetime.datetime.utcnow().date()
    return f'{today.isoformat()}T00:00:00Z'


def _count_actions_today(conn, owner: str, action: str) -> int:
    return (conn.execute(
        'SELECT COUNT(*) AS n FROM ig_growth_actions '
        'WHERE owner = ? AND action = ? AND created_at >= ? AND success = 1',
        (owner, action, _today_iso_start()),
    ).fetchone() or {'n': 0})['n']


@app.route('/api/ig/growth/dashboard', methods=['GET'])
@require_auth
def ig_growth_dashboard():
    owner = request.user['username']
    with get_db() as conn:
        settings_row = _ensure_settings_row(conn, owner)
        stories_today = _count_actions_today(conn, owner, 'view_story')
        follows_today = _count_actions_today(conn, owner, 'follow')
        likes_today = _count_actions_today(conn, owner, 'like')
        # Candidatas pendientes y convertidas
        pending = conn.execute(
            'SELECT COUNT(*) AS n FROM ig_growth_candidates '
            'WHERE owner = ? AND status = ? AND score >= ?',
            (owner, 'pending', settings_row.get('min_score') or 60),
        ).fetchone()['n']
        engaged = conn.execute(
            'SELECT COUNT(*) AS n FROM ig_growth_candidates '
            'WHERE owner = ? AND status = ?',
            (owner, 'engaged'),
        ).fetchone()['n']
        converted = conn.execute(
            'SELECT COUNT(*) AS n FROM ig_growth_candidates '
            'WHERE owner = ? AND status = ?',
            (owner, 'converted'),
        ).fetchone()['n']
        # Próxima task encolada para este owner
        next_task = conn.execute(
            "SELECT id, type, status, created_at FROM tasks "
            "WHERE owner = ? AND type IN ('ig_growth_discover','ig_growth_view_stories') "
            "AND status IN ('queued','running') "
            "ORDER BY id ASC LIMIT 1",
            (owner,),
        ).fetchone()
    return jsonify({
        'settings': _settings_to_dict(dict(settings_row)),
        'today': {
            'stories_viewed': stories_today,
            'follows': follows_today,
            'likes': likes_today,
        },
        'candidates': {
            'pending_min_score': pending,
            'engaged': engaged,
            'converted': converted,
        },
        'next_task': dict(next_task) if next_task else None,
        'orchestrator': {
            'disabled_by_env': os.environ.get('GROWTH_ORCHESTRATOR_DISABLED', '').lower() == 'true',
            'thread_started': _orchestrator_started,
        },
    })


@app.route('/api/ig/growth/actions/log', methods=['POST'])
@require_worker
def ig_growth_log_action():
    """Worker reporta acciones individuales (view_story, like, follow...)."""
    data = request.get_json(silent=True) or {}
    owner = (data.get('owner') or '').strip()
    candidate_id = data.get('candidate_id')
    ig_pk = (data.get('ig_pk') or '').strip()
    action = (data.get('action') or '').strip()
    success = 1 if data.get('success', True) else 0
    error = data.get('error')
    if not owner or not action:
        return jsonify({'message': 'Body inválido.'}), 400
    if action not in ('view_story', 'like', 'follow', 'unfollow', 'comment', 'dm'):
        return jsonify({'message': f'action inválida: {action}'}), 400
    with get_db() as conn:
        conn.execute(
            'INSERT INTO ig_growth_actions '
            '(owner, candidate_id, ig_pk, action, success, error, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?)',
            (owner, candidate_id, ig_pk or None, action, success, error, now_iso()),
        )
        # Actualiza el status de la candidata si fue acción exitosa
        if success and candidate_id:
            new_status = {
                'view_story': 'engaged',
                'like': 'engaged',
                'follow': 'followed',
                'unfollow': 'unfollowed',
            }.get(action)
            if new_status:
                conn.execute(
                    'UPDATE ig_growth_candidates SET status = ?, last_action_at = ? '
                    'WHERE id = ? AND owner = ?',
                    (new_status, now_iso(), candidate_id, owner),
                )
        conn.commit()
    return jsonify({'ok': True})


# --- Orquestador (thread daemon en cada proceso gunicorn) ------------

def _try_acquire_lock(name: str, ttl_seconds: int = 90) -> bool:
    """Mutex distribuido vía SQLite. Solo un proceso ejecuta el tick a la vez."""
    now = datetime.datetime.utcnow()
    expiry = (now + datetime.timedelta(seconds=ttl_seconds)).isoformat(timespec='seconds') + 'Z'
    cutoff = now.isoformat(timespec='seconds') + 'Z'
    holder_id = f'pid={os.getpid()}'
    with get_db() as conn:
        # Intenta UPDATE si existe y caducó
        cur = conn.execute(
            'UPDATE app_locks SET holder = ?, lease_until = ? '
            'WHERE name = ? AND (lease_until IS NULL OR lease_until < ?)',
            (holder_id, expiry, name, cutoff),
        )
        if cur.rowcount > 0:
            conn.commit()
            return True
        # Intenta INSERT si no existía
        try:
            conn.execute(
                'INSERT INTO app_locks (name, holder, lease_until) VALUES (?, ?, ?)',
                (name, holder_id, expiry),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False


def _is_in_active_hours(start: int, end: int) -> bool:
    """Hora actual UTC en [start, end). Acepta wraparound (e.g. 22..6)."""
    h = datetime.datetime.utcnow().hour
    if start <= end:
        return start <= h < end
    return h >= start or h < end  # wrap


def _next_source_round_robin(conn, owner: str):
    """Coge la próxima source enabled (orden: priority asc, last_run_at oldest)."""
    return conn.execute(
        "SELECT * FROM ig_growth_sources WHERE owner = ? AND enabled = 1 "
        "ORDER BY priority ASC, COALESCE(last_run_at, '1970-01-01') ASC LIMIT 1",
        (owner,),
    ).fetchone()


def _has_pending_growth_task(conn, owner: str, type_: str) -> bool:
    return bool(conn.execute(
        "SELECT 1 FROM tasks WHERE owner = ? AND type = ? AND status IN ('queued','running') LIMIT 1",
        (owner, type_),
    ).fetchone())


def _enqueue_task(conn, owner: str, task_type: str, payload: dict, started: bool = False):
    """Inserta una task SIN cobrar tokens (uso del orquestador)."""
    payload_json = json.dumps(payload) if payload else None
    status = 'running' if started else 'queued'
    cur = conn.execute(
        'INSERT INTO tasks (owner, type, status, created_at, payload, started_at) '
        'VALUES (?, ?, ?, ?, ?, ?)',
        (owner, task_type, status, now_iso(), payload_json,
         now_iso() if started else None),
    )
    return cur.lastrowid


def _orchestrate_for_owner(conn, owner: str):
    settings = _ensure_settings_row(conn, owner)
    if not settings.get('auto_enabled'):
        return
    paused_until = settings.get('paused_until')
    if paused_until and paused_until > now_iso():
        return
    if not _is_in_active_hours(settings['active_hours_start'], settings['active_hours_end']):
        return

    # ¿Toca discovery?
    last_disc = settings.get('last_discovery_at') or '1970-01-01T00:00:00Z'
    interval_disc = datetime.timedelta(minutes=settings.get('discovery_interval_minutes') or 180)
    try:
        last_disc_dt = datetime.datetime.fromisoformat(last_disc.rstrip('Z'))
    except Exception:
        last_disc_dt = datetime.datetime(1970, 1, 1)
    if datetime.datetime.utcnow() - last_disc_dt >= interval_disc:
        # No solapamos discoveries: si ya hay uno encolado/corriendo, saltamos
        if not _has_pending_growth_task(conn, owner, 'ig_growth_discover'):
            src = _next_source_round_robin(conn, owner)
            if src:
                _enqueue_task(conn, owner, 'ig_growth_discover', {
                    'source_id': src['id'],
                    'source_detail': {
                        'id': src['id'],
                        'kind': src['kind'],
                        'value': src['value'],
                        'niche': src['niche'],
                    },
                    'max_candidates': 30,
                    'min_score': settings.get('min_score') or 60,
                })
                conn.execute(
                    'UPDATE ig_growth_settings SET last_discovery_at = ? WHERE owner = ?',
                    (now_iso(), owner),
                )
                log.info('orchestrator: encolé discovery #%s para %s sobre %s=%s',
                         src['id'], owner, src['kind'], src['value'])

    # ¿Toca engagement?
    last_eng = settings.get('last_engagement_at') or '1970-01-01T00:00:00Z'
    interval_eng = datetime.timedelta(minutes=settings.get('engagement_interval_minutes') or 25)
    try:
        last_eng_dt = datetime.datetime.fromisoformat(last_eng.rstrip('Z'))
    except Exception:
        last_eng_dt = datetime.datetime(1970, 1, 1)
    if datetime.datetime.utcnow() - last_eng_dt >= interval_eng:
        if _has_pending_growth_task(conn, owner, 'ig_growth_view_stories'):
            return  # ya hay uno en cola, no solapamos
        # Comprueba cuota
        stories_today = _count_actions_today(conn, owner, 'view_story')
        if stories_today >= (settings.get('daily_view_stories_limit') or 50):
            return
        # Coge top candidatas pending dentro del min_score
        budget_left = (settings.get('daily_view_stories_limit') or 50) - stories_today
        batch = min(budget_left, 6)  # batch razonable por task (6-10)
        cands = conn.execute(
            'SELECT id, ig_pk, ig_username FROM ig_growth_candidates '
            'WHERE owner = ? AND status = ? AND score >= ? '
            'ORDER BY score DESC, discovered_at ASC LIMIT ?',
            (owner, 'pending', settings.get('min_score') or 60, batch),
        ).fetchall()
        if not cands:
            return
        _enqueue_task(conn, owner, 'ig_growth_view_stories', {
            'candidates': [{
                'candidate_id': c['id'],
                'ig_pk': c['ig_pk'],
                'ig_username': c['ig_username'],
            } for c in cands],
        })
        conn.execute(
            'UPDATE ig_growth_settings SET last_engagement_at = ? WHERE owner = ?',
            (now_iso(), owner),
        )
        log.info('orchestrator: encolé view_stories de %d candidatas para %s',
                 len(cands), owner)


def _orchestrator_tick():
    if not _try_acquire_lock('growth_orchestrator', ttl_seconds=90):
        return  # otro proceso lo está haciendo
    with get_db() as conn:
        owners = conn.execute(
            'SELECT owner FROM ig_growth_settings WHERE auto_enabled = 1',
        ).fetchall()
        for o in owners:
            try:
                _orchestrate_for_owner(conn, o['owner'])
                conn.commit()
            except Exception:
                log.exception('orchestrator tick fallo para %s', o['owner'])


_orchestrator_started = False
_orchestrator_lock = threading.Lock()


def _orchestrator_loop():
    # Espera inicial: deja que init_db termine, que gunicorn esté servido y
    # que el primer health-check pase. Si arrancamos a los 0s podemos
    # competir con peticiones HTTP por el SQLite y dar 500 en /api/* hasta
    # que el server estabiliza.
    time.sleep(30)
    log.info('Growth orchestrator started (pid=%d)', os.getpid())
    while True:
        try:
            _orchestrator_tick()
        except sqlite3.OperationalError as e:
            # No-op gracefully si el schema todavía no está completo
            # (caso típico al primer arranque antes de init_db terminar) o
            # si la DB está bloqueada. Los siguientes ticks reintentarán.
            log.warning('orchestrator tick: SQLite operational: %s', e)
        except Exception:
            log.exception('orchestrator loop crashed (continuing)')
        time.sleep(60)


def start_orchestrator():
    global _orchestrator_started
    if os.environ.get('GROWTH_ORCHESTRATOR_DISABLED', '').lower() == 'true':
        log.info('Growth orchestrator disabled by env var.')
        return
    with _orchestrator_lock:
        if _orchestrator_started:
            return
        _orchestrator_started = True
        try:
            t = threading.Thread(
                target=_orchestrator_loop, daemon=True,
                name='growth-orchestrator',
            )
            t.start()
        except Exception:
            # Nunca debe matar el arranque del web server.
            log.exception('No se pudo arrancar el orchestrator (continuando sin él)')
            _orchestrator_started = False


# --- Digital Footprint ---
@app.route('/api/footprint/latest', methods=['GET'])
@require_auth
def footprint_latest():
    """Último scan del owner."""
    with get_db() as conn:
        row = conn.execute(
            'SELECT id, created_at, email_masked, phone_masked, face_used, score, result_json, task_id '
            'FROM footprint_scans WHERE owner = ? ORDER BY created_at DESC LIMIT 1',
            (request.user['username'],),
        ).fetchone()
    if not row:
        return jsonify({'scan': None})
    try:
        result = json.loads(row['result_json']) if row['result_json'] else {}
    except Exception:
        result = {}
    return jsonify({'scan': {
        'id': row['id'],
        'created_at': row['created_at'],
        'email_masked': row['email_masked'],
        'phone_masked': row['phone_masked'],
        'face_used': bool(row['face_used']),
        'score': row['score'],
        'result': result,
        'task_id': row['task_id'],
    }})


@app.route('/api/footprint/history', methods=['GET'])
@require_auth
def footprint_history():
    """Histórico de scans del owner (sin el JSON completo, solo metadatos)."""
    try:
        limit = max(1, min(int(request.args.get('limit', 30)), 200))
    except ValueError:
        limit = 30
    with get_db() as conn:
        rows = conn.execute(
            'SELECT id, created_at, email_masked, phone_masked, face_used, score, task_id '
            'FROM footprint_scans WHERE owner = ? ORDER BY created_at DESC LIMIT ?',
            (request.user['username'], limit),
        ).fetchall()
    return jsonify({'history': [{
        'id': r['id'],
        'created_at': r['created_at'],
        'email_masked': r['email_masked'],
        'phone_masked': r['phone_masked'],
        'face_used': bool(r['face_used']),
        'score': r['score'],
        'task_id': r['task_id'],
    } for r in rows]})


@app.route('/api/footprint/scan/<int:scan_id>', methods=['GET'])
@require_auth
def footprint_get(scan_id):
    with get_db() as conn:
        row = conn.execute(
            'SELECT * FROM footprint_scans WHERE id = ? AND owner = ?',
            (scan_id, request.user['username']),
        ).fetchone()
    if not row:
        return jsonify({'message': 'No encontrado'}), 404
    try:
        result = json.loads(row['result_json']) if row['result_json'] else {}
    except Exception:
        result = {}
    return jsonify({'scan': {
        'id': row['id'],
        'created_at': row['created_at'],
        'email_masked': row['email_masked'],
        'phone_masked': row['phone_masked'],
        'face_used': bool(row['face_used']),
        'score': row['score'],
        'result': result,
        'task_id': row['task_id'],
    }})


# --- Admin ---
@app.route('/api/admin/users', methods=['GET'])
@require_admin
def admin_list_users():
    with get_db() as conn:
        rows = conn.execute('SELECT username, tokens, rol FROM usuarios ORDER BY username').fetchall()
    return jsonify({'usuarios': [dict(r) for r in rows]})


@app.route('/api/update-user', methods=['POST'])
@require_admin
def admin_update_user():
    data = request.get_json(silent=True) or {}
    username = (data.get('username') or '').strip()
    if not username:
        return jsonify({'message': 'username es obligatorio'}), 400

    if not get_user(username):
        return jsonify({'message': 'Usuario no encontrado'}), 404

    sets, params = [], []
    if 'tokens' in data:
        try:
            sets.append('tokens = ?'); params.append(int(data['tokens']))
        except (TypeError, ValueError):
            return jsonify({'message': 'tokens debe ser un entero'}), 400
    if 'rol' in data:
        if data['rol'] not in ('user', 'admin'):
            return jsonify({'message': "rol debe ser 'user' o 'admin'"}), 400
        sets.append('rol = ?'); params.append(data['rol'])

    if not sets:
        return jsonify({'message': 'Nada que actualizar'}), 400

    params.append(username)
    with get_db() as conn:
        conn.execute(f'UPDATE usuarios SET {", ".join(sets)} WHERE username = ?', params)
        conn.commit()
    log.info('admin %s updated %s: %s', request.user['username'], username, data)
    return jsonify({'message': 'Usuario actualizado'})


@app.route('/api/admin/cleanup-zombies', methods=['POST'])
@require_admin
def admin_cleanup_zombies():
    """Fuerza el barrido de tareas zombie."""
    with get_db() as conn:
        n = cleanup_zombie_tasks(conn)
        conn.commit()
    return jsonify({'failed': n, 'message': f'{n} tareas marcadas como failed.'})


@app.route('/api/admin/grant-tokens', methods=['POST'])
@require_admin
def admin_grant_tokens():
    data = request.get_json(silent=True) or {}
    username = (data.get('username') or '').strip()
    try:
        amount = int(data.get('amount'))
    except (TypeError, ValueError):
        return jsonify({'message': 'amount debe ser un entero'}), 400
    note = data.get('note') or f"Concedido por admin {request.user['username']}"
    if not username:
        return jsonify({'message': 'username es obligatorio'}), 400
    if amount == 0:
        return jsonify({'message': 'amount no puede ser 0'}), 400
    if not get_user(username):
        return jsonify({'message': 'Usuario no encontrado'}), 404

    if amount > 0:
        ok, balance = credit_tokens(username, amount, 'grant', note=note)
        return jsonify({'ok': ok, 'balance': balance})
    # amount negativo: regalo inverso (corrección)
    with get_db() as conn:
        conn.execute('UPDATE usuarios SET tokens = MAX(0, tokens + ?) WHERE username = ?', (amount, username))
        row = conn.execute('SELECT tokens FROM usuarios WHERE username = ?', (username,)).fetchone()
        balance = row['tokens'] if row else 0
        add_transaction(conn, username, 'refund', amount, balance, note=note)
        conn.commit()
    return jsonify({'ok': True, 'balance': balance})


@app.route('/api/admin/delete-user/<username>', methods=['DELETE'])
@require_admin
def admin_delete_user(username: str):
    if username == request.user['username']:
        return jsonify({'message': 'No puedes eliminar tu propia cuenta'}), 400

    with get_db() as conn:
        cur = conn.execute('DELETE FROM usuarios WHERE username = ?', (username,))
        conn.commit()
        if cur.rowcount == 0:
            return jsonify({'message': 'Usuario no encontrado'}), 404
    log.info('admin %s deleted user %s', request.user['username'], username)
    return jsonify({'message': 'Usuario eliminado'})


# --- Health / errores ---
@app.route('/')
def home():
    return '🔥 HokoSocial API activa'


@app.errorhandler(404)
def not_found(_):
    return jsonify({'message': 'Recurso no encontrado'}), 404


@app.errorhandler(405)
def method_not_allowed(_):
    return jsonify({'message': 'Método no permitido'}), 405


@app.errorhandler(500)
def server_error(e):
    log.exception('500: %s', e)
    return jsonify({'message': 'Error interno del servidor'}), 500


# Bootstrap se ejecuta tras tener todas las funciones definidas (hash_password,
# add_transaction). Va al final del módulo a propósito.
bootstrap_admin()
start_orchestrator()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug)
