from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
import sqlite3
import jwt
import datetime
import os

# Configuración
app = Flask(__name__)
app.config['SECRET_KEY'] = 'clave-secreta-segura'  # Cámbiala en producción
DB_PATH = os.path.join('/tmp', 'usuarios.db')

# CORS para permitir cookies cross-origin (Vercel <-> Render)
CORS(app, supports_credentials=True, resources={r"/*": {"origins": "*"}})

# Crear tabla usuarios si no existe
def crear_tabla():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS usuarios (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT UNIQUE,
                        password TEXT
                    )''')
        # Añadir columnas si no existen
        try: c.execute("ALTER TABLE usuarios ADD COLUMN tokens INTEGER DEFAULT 10")
        except: pass
        try: c.execute("ALTER TABLE usuarios ADD COLUMN rol TEXT DEFAULT 'user'")
        except: pass
        conn.commit()

crear_tabla()

# Registro
@app.route('/api/register', methods=['POST'])
def register():
    try:
        data = request.json
        username = data.get('username')
        password = data.get('password')
        rol = 'admin' if username == 'admin' else 'user'

        if not username or not password:
            return jsonify({"message": "Faltan datos"}), 400

        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("INSERT INTO usuarios (username, password, tokens, rol) VALUES (?, ?, ?, ?)",
                      (username, password, 10, rol))
            conn.commit()
            return jsonify({"message": "Usuario registrado correctamente"}), 201
    except sqlite3.IntegrityError:
        return jsonify({"message": "El usuario ya existe"}), 409
    except Exception as e:
        print("⚠️ Error en /api/register:", e)
        return jsonify({"error": str(e)}), 500

# Login
@app.route('/api/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')

    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT username, rol FROM usuarios WHERE username=? AND password=?", (username, password))
        user = c.fetchone()

    if user:
        token = jwt.encode({
            'username': user[0],
            'rol': user[1],
            'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=12)
        }, app.config['SECRET_KEY'], algorithm='HS256')

        resp = make_response(jsonify({'message': 'Login exitoso', 'token': token}))
        resp.set_cookie('token', token, httponly=True, samesite='None', secure=True)
        return resp
    else:
        return jsonify({'message': 'Credenciales incorrectas'}), 401

# Ruta protegida: datos del usuario
@app.route('/api/user-data', methods=['GET'])
def user_data():
    token = request.headers.get('Authorization')
    if not token:
        return jsonify({'message': 'Token requerido'}), 401

    try:
        decoded = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
        username = decoded['username']
        rol = decoded['rol']

        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("SELECT tokens FROM usuarios WHERE username=?", (username,))
            result = c.fetchone()

        return jsonify({
            'username': username,
            'rol': rol,
            'tokens': result[0] if result else 0
        }), 200
    except jwt.ExpiredSignatureError:
        return jsonify({'message': 'Token expirado'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'message': 'Token inválido'}), 401

# Ejecutar bot (consume token)
@app.route('/api/run-bot', methods=['POST'])
def run_bot():
    token = request.headers.get('Authorization')
    if not token:
        return jsonify({'message': 'Token requerido'}), 401

    try:
        decoded = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
        username = decoded['username']

        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("SELECT tokens FROM usuarios WHERE username=?", (username,))
            tokens_actuales = c.fetchone()[0]

            if tokens_actuales > 0:
                nuevos_tokens = tokens_actuales - 1
                c.execute("UPDATE usuarios SET tokens=? WHERE username=?", (nuevos_tokens, username))
                conn.commit()
                return jsonify({'message': 'Bot ejecutado', 'tokens_restantes': nuevos_tokens}), 200
            else:
                return jsonify({'message': 'No tienes tokens'}), 400
    except jwt.InvalidTokenError:
        return jsonify({'message': 'Token inválido'}), 401

# Ver todos los usuarios (admin only)
@app.route('/api/usuarios', methods=['GET'])
def get_usuarios():
    token = request.headers.get('Authorization')
    if not token:
        return jsonify({'message': 'Token requerido'}), 401

    try:
        decoded = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
        if decoded.get('rol') != 'admin':
            return jsonify({'message': 'No autorizado'}), 403

        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("SELECT id, username, rol, tokens FROM usuarios")
            users = [{'id': row[0], 'username': row[1], 'rol': row[2], 'tokens': row[3]} for row in c.fetchall()]
            return jsonify({'usuarios': users})
    except jwt.InvalidTokenError:
        return jsonify({'message': 'Token inválido'}), 401

# Eliminar usuario (admin only)
@app.route('/api/eliminar-usuario', methods=['POST'])
def eliminar_usuario():
    token = request.headers.get('Authorization')
    if not token:
        return jsonify({'message': 'Token requerido'}), 401

    try:
        decoded = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
        if decoded.get('rol') != 'admin':
            return jsonify({'message': 'No autorizado'}), 403

        data = request.json
        user_id = data.get('id')

        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("DELETE FROM usuarios WHERE id=?", (user_id,))
            conn.commit()
        return jsonify({'message': 'Usuario eliminado correctamente'})
    except Exception as e:
        return jsonify({'message': f'Error: {e}'}), 500

# Logout
@app.route('/api/logout', methods=['GET'])
def logout():
    resp = make_response(jsonify({'message': 'Sesión cerrada'}))
    resp.set_cookie('token', '', expires=0)
    return resp

# Simulación de log del bot
@app.route('/api/log', methods=['GET'])
def get_log():
    return jsonify({
        'log_lines': ['[00:00] Bot aún no ejecutado.'],
        'mensaje_bot': 'Esperando acción del usuario...'
    })

# Test de vida
@app.route('/')
def home():
    return 'API funcionando correctamente 🔥 - MAAX 𒉭'

# Headers extra CORS
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

# Ejecutar
if __name__ == '__main__':
    app.run(debug=True)
