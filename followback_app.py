from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
import sqlite3
import jwt
import datetime

# Configuración
app = Flask(__name__)
app.config['SECRET_KEY'] = 'clave-secreta-segura'  # Cámbiala en producción
DB_PATH = 'usuarios.db'

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
        conn.commit()

crear_tabla()

# Registro
@app.route('/api/register', methods=['POST'])
def register():
    data = request.json
    username = data.get('username')
    password = data.get('password')

    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        try:
            c.execute("INSERT INTO usuarios (username, password) VALUES (?, ?)", (username, password))
            conn.commit()
            return jsonify({"message": "Usuario registrado correctamente"}), 201
        except sqlite3.IntegrityError:
            return jsonify({"message": "El usuario ya existe"}), 409

# Login
@app.route('/api/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')

    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM usuarios WHERE username=? AND password=?", (username, password))
        user = c.fetchone()

    if user:
        token = jwt.encode({
            'username': username,
            'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=12)
        }, app.config['SECRET_KEY'], algorithm='HS256')

        resp = make_response(jsonify({'message': 'Login exitoso'}))
        # Cookie con JWT (necesaria para credentials: 'include')
        resp.set_cookie('token', token, httponly=True, samesite='None', secure=True)
        return resp
    else:
        return jsonify({'message': 'Credenciales incorrectas'}), 401

# Ruta protegida (devuelve datos del usuario)
@app.route('/api/user-data', methods=['GET'])
def user_data():
    token = request.headers.get('Authorization')
    if not token:
        return jsonify({'message': 'Token requerido'}), 401

    try:
        decoded = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
        username = decoded['username']

        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("SELECT tokens FROM usuarios WHERE username=?", (username,))
            result = c.fetchone()

        return jsonify({
            'username': username,
            'tokens': result[0] if result else 0
        }), 200
    except jwt.ExpiredSignatureError:
        return jsonify({'message': 'Token expirado'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'message': 'Token inválido'}), 401

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

# Logout: borra cookie
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
