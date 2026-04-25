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

TASK_TYPES = ('search', 'followback', 'simulate', 'instagram_profile')
WORKER_TYPES = ('search', 'followback', 'instagram_profile')
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
    conn = sqlite3.connect(DB_PATH)
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


def consume_one_token(username: str, note: str | None = None) -> tuple[bool, int]:
    """Decrementa 1 token + crea transaction kind='consume'. Atómico."""
    with get_db() as conn:
        cur = conn.execute(
            'UPDATE usuarios SET tokens = tokens - 1 WHERE username = ? AND tokens > 0',
            (username,),
        )
        row = conn.execute(
            'SELECT tokens FROM usuarios WHERE username = ?', (username,)
        ).fetchone()
        balance = row['tokens'] if row else 0
        if cur.rowcount > 0:
            add_transaction(conn, username, 'consume', -1, balance, note=note)
            conn.commit()
            return True, balance
        conn.commit()
    return False, balance


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

    username = request.user['username']
    ok, tokens_restantes = consume_one_token(username, note=f'Tarea {task_type}')
    if not ok:
        return jsonify({
            'message': 'No tienes tokens disponibles',
            'tokens_restantes': tokens_restantes,
        }), 402

    payload_json = json.dumps(payload) if payload is not None else None

    with get_db() as conn:
        cur = conn.execute(
            'INSERT INTO tasks (owner, type, status, created_at, payload) VALUES (?, ?, ?, ?, ?)',
            (username, task_type, 'queued', now_iso(), payload_json),
        )
        conn.commit()
        task_id = cur.lastrowid

    log.info('queued task %s type=%s for %s', task_id, task_type, username)

    if task_type == 'simulate':
        with get_db() as conn:
            conn.execute(
                "UPDATE tasks SET status='running', started_at=? WHERE id=?",
                (now_iso(), task_id),
            )
            conn.commit()
        threading.Thread(target=run_simulation, args=(task_id,), daemon=True).start()

    return jsonify({
        'id': task_id,
        'type': task_type,
        'status': 'running' if task_type == 'simulate' else 'queued',
        'tokens_restantes': tokens_restantes,
        'message': 'Tarea encolada.' if task_type != 'simulate' else 'Simulación lanzada.',
    }), 201


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
    """El worker guarda un dict de resultado estructurado (ej. perfil Instagram)."""
    data = request.get_json(silent=True) or {}
    result = data.get('result')
    if result is None:
        return jsonify({'message': 'Falta result.'}), 400
    try:
        serialized = json.dumps(result)
    except Exception:
        return jsonify({'message': 'result no serializable a JSON'}), 400
    with get_db() as conn:
        cur = conn.execute('UPDATE tasks SET result = ? WHERE id = ?', (serialized, task_id))
        conn.execute('UPDATE worker_status SET last_seen = ? WHERE id = 1', (now_iso(),))
        conn.commit()
    if cur.rowcount == 0:
        return jsonify({'message': 'Tarea no encontrada'}), 404
    return jsonify({'ok': True})


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


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug)
