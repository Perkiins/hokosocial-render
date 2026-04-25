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

TASK_TYPES = ('search', 'followback', 'simulate')
WORKER_TYPES = ('search', 'followback')
MAX_LOG_LINES = 2000
WORKER_OFFLINE_AFTER = 30

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

        conn.commit()


init_db()


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
    if task_type not in TASK_TYPES:
        return jsonify({'message': f"type debe ser uno de: {', '.join(TASK_TYPES)}"}), 400

    username = request.user['username']
    ok, tokens_restantes = consume_one_token(username, note=f'Tarea {task_type}')
    if not ok:
        return jsonify({
            'message': 'No tienes tokens disponibles',
            'tokens_restantes': tokens_restantes,
        }), 402

    with get_db() as conn:
        cur = conn.execute(
            'INSERT INTO tasks (owner, type, status, created_at) VALUES (?, ?, ?, ?)',
            (username, task_type, 'queued', now_iso()),
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


# --- Worker — heartbeat y cola ---
@app.route('/api/worker/heartbeat', methods=['POST'])
@require_worker
def worker_heartbeat():
    with get_db() as conn:
        conn.execute('UPDATE worker_status SET last_seen = ? WHERE id = 1', (now_iso(),))
        conn.commit()
    return jsonify({'ok': True, 'server_time': now_iso()})


@app.route('/api/worker/next', methods=['POST'])
@require_worker
def worker_next():
    placeholders = ','.join('?' for _ in WORKER_TYPES)
    with get_db() as conn:
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


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug)
