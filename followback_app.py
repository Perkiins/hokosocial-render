import datetime
import logging
import os
import sqlite3
import threading
from functools import wraps

import jwt
from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.security import check_password_hash, generate_password_hash

# --- Config ---
app = Flask(__name__)

SECRET_KEY = os.environ.get('SECRET_KEY')
if not SECRET_KEY:
    # Fallback dev. En Render debe estar definido SIEMPRE.
    SECRET_KEY = 'dev-only-not-for-production'
    app.logger.warning('SECRET_KEY no está definido en entorno — usando fallback dev.')
app.config['SECRET_KEY'] = SECRET_KEY

cors_origins = [o.strip() for o in os.environ.get('CORS_ORIGINS', 'https://hokosocial.vercel.app').split(',') if o.strip()]
CORS(app, origins=cors_origins, supports_credentials=True)

DB_PATH = os.environ.get('DB_PATH', 'usuarios.db')

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


def crear_tabla():
    with get_db() as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS usuarios (
            username TEXT PRIMARY KEY,
            password TEXT,
            tokens INTEGER DEFAULT 5,
            rol TEXT DEFAULT 'user'
        )''')


crear_tabla()


# --- Password hashing (compatible con passwords legacy en texto plano) ---
def hash_password(plain: str) -> str:
    return generate_password_hash(plain)


def verify_password(plain: str, stored: str) -> tuple[bool, bool]:
    """Devuelve (ok, needs_rehash). needs_rehash=True si el stored era texto plano."""
    if not stored:
        return False, False
    if stored.startswith(('pbkdf2:', 'scrypt:', 'argon2')):
        return check_password_hash(stored, plain), False
    # Legacy: texto plano. Comparación directa y marcamos para rehash.
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
        log.info('JWT expirado')
        return None
    except jwt.InvalidTokenError as e:
        log.warning('JWT inválido: %s', e)
        return None


def get_user(username: str):
    with get_db() as conn:
        row = conn.execute(
            'SELECT username, password, tokens, rol FROM usuarios WHERE username = ?',
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


# --- Auth endpoints ---
@app.route('/api/register', methods=['POST'])
def register():
    data = request.get_json(silent=True) or {}
    username = (data.get('username') or '').strip()
    password = data.get('password') or ''
    if not username or not password:
        return jsonify({'message': 'Usuario y contraseña son obligatorios'}), 400
    if len(password) < 6:
        return jsonify({'message': 'La contraseña debe tener al menos 6 caracteres'}), 400

    try:
        with get_db() as conn:
            conn.execute(
                'INSERT INTO usuarios (username, password) VALUES (?, ?)',
                (username, hash_password(password)),
            )
            conn.commit()
    except sqlite3.IntegrityError:
        return jsonify({'message': 'Usuario ya existe'}), 409
    except Exception:
        log.exception('register failed for %s', username)
        return jsonify({'message': 'Error al registrar'}), 500

    log.info('register ok: %s', username)
    return jsonify({'token': generar_token(username)}), 201


@app.route('/api/login', methods=['POST'])
def login():
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
    return jsonify({'username': u['username'], 'tokens': u['tokens'], 'rol': u['rol']})


# --- Bot ---
@app.route('/api/run-bot', methods=['POST'])
@require_auth
def run_bot():
    username = request.user['username']

    # Decrementamos atómicamente; si no había tokens, no lanzamos.
    with get_db() as conn:
        cur = conn.execute(
            'UPDATE usuarios SET tokens = tokens - 1 WHERE username = ? AND tokens > 0',
            (username,),
        )
        conn.commit()
        if cur.rowcount == 0:
            row = conn.execute('SELECT tokens FROM usuarios WHERE username = ?', (username,)).fetchone()
            return jsonify({'message': 'No tienes tokens disponibles', 'tokens_restantes': row['tokens'] if row else 0}), 402
        row = conn.execute('SELECT tokens FROM usuarios WHERE username = ?', (username,)).fetchone()
        tokens_restantes = row['tokens'] if row else 0

    log_file = f'log_{username}.txt'

    def tarea():
        try:
            import time
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write('🔄 Ejecutando bot...\n')
                for i in range(3):
                    f.write(f'⏳ Proceso {i+1}/3\n')
                    f.flush()
                    time.sleep(1)
                f.write('✅ Bot finalizado\n')
        except Exception:
            log.exception('bot task failed for %s', username)

    threading.Thread(target=tarea, daemon=True).start()
    return jsonify({'message': 'Bot lanzado correctamente', 'tokens_restantes': tokens_restantes})


@app.route('/api/log', methods=['GET'])
@require_auth
def get_log():
    username = request.user['username']
    log_file = f'log_{username}.txt'
    if not os.path.exists(log_file):
        return jsonify({'log': [], 'mensaje_bot': 'Sin log aún.'})

    with open(log_file, 'r', encoding='utf-8') as f:
        lines = [l.strip() for l in f.readlines() if l.strip()]
    return jsonify({
        'log': lines,
        'mensaje_bot': lines[-1] if lines else 'Sin mensaje final',
    })


@app.route('/api/generar-cookies', methods=['POST'])
@require_auth
def generar_cookies():
    # Stub honesto: el flujo de cookies aún no está implementado en el backend.
    return jsonify({'message': 'La generación de cookies aún no está implementada en el servidor.'}), 501


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
