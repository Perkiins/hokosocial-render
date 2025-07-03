from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
import sqlite3
import jwt
import datetime
import os

# Configuración
app = Flask(__name__)
app.config['SECRET_KEY'] = 'clave-secreta-segura'
DB_PATH = os.path.join(os.getcwd(), 'usuarios.db')

# CORS
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

        if not username or not password:
            return jsonify({"message": "Faltan datos"}), 400

        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("INSERT INTO usuarios (username, password, tokens, rol) VALUES (?, ?, ?, ?)",
                      (username, password, 10, 'user'))
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
        c.execute("SELECT * FROM usuarios WHERE username=? AND password=?", (username, password))
        user = c.fetchone()

    if user:
        token = jwt.encode({
            'username': username,
            'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=12)
        }, app.config['SECRET_KEY'], algorithm='HS256')

        resp = make_response(jsonify({'message': 'Login exitoso'}))
        resp.set_cookie('token', token, httponly=True, samesite='None', secure=True)
        return resp
    else:
        return jsonify({'message': 'Credenciales incorrectas'}), 401

# Ruta protegida (datos del usuario)
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
            c.execute("SELECT tokens, rol FROM usuarios WHERE username=?", (username,))
            result = c.fetchone()

        return jsonify({
            'username': username,
            'tokens': result[0] if result else 0,
            'rol': result[1] if result else 'user'
        }), 200
    except jwt.ExpiredSignatureError:
        return jsonify({'message': 'Token expirado'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'message': 'Token inválido'}), 401

# Ejecutar bot
@app.route('/api/run-bot', methods=['POST'])
def run_bot():
    token = request.cookies.get('token')
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

# Logout
@app.route('/api/logout', methods=['GET'])
def logout():
    resp = make_response(jsonify({'message': 'Sesión cerrada'}))
    resp.set_cookie('token', '', expires=0)
    return resp

# Ver todos los usuarios (solo admin)
@app.route('/api/users', methods=['GET'])
def get_users():
    token = request.cookies.get('token')
    if not token:
        return jsonify({'message': 'Token requerido'}), 401

    try:
        decoded = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
        username = decoded['username']

        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("SELECT rol FROM usuarios WHERE username=?", (username,))
            rol = c.fetchone()[0]

            if rol != 'admin':
                return jsonify({'message': 'No autorizado'}), 403

            c.execute("SELECT id, username, tokens, rol FROM usuarios")
            users = c.fetchall()
            users_list = [{'id': u[0], 'username': u[1], 'tokens': u[2], 'rol': u[3]} for u in users]
            return jsonify(users_list)
    except Exception as e:
        print("⚠️ Error en /api/users:", e)
        return jsonify({'error': str(e)}), 500

# Eliminar usuario (solo admin)
@app.route('/api/delete-user', methods=['POST'])
def delete_user():
    token = request.cookies.get('token')
    if not token:
        return jsonify({'message': 'Token requerido'}), 401

    try:
        decoded = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
        username_admin = decoded['username']

        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("SELECT rol FROM usuarios WHERE username=?", (username_admin,))
            rol = c.fetchone()[0]

            if rol != 'admin':
                return jsonify({'message': 'No autorizado'}), 403

            data = request.json
            user_id = data.get('id')
            if not user_id:
                return jsonify({'message': 'ID requerido'}), 400

            c.execute("DELETE FROM usuarios WHERE id=?", (user_id,))
            conn.commit()
            return jsonify({'message': 'Usuario eliminado'}), 200
    except Exception as e:
        print("⚠️ Error en /api/delete-user:", e)
        return jsonify({'error': str(e)}), 500

@app.route('/api/update-user', methods=['POST'])
def update_user():
    token = request.cookies.get('token')
    if not token:
        return jsonify({'message': 'Token requerido'}), 401

    try:
        decoded = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
        username_admin = decoded['username']

        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("SELECT rol FROM usuarios WHERE username=?", (username_admin,))
            rol = c.fetchone()[0]

            if rol != 'admin':
                return jsonify({'message': 'No autorizado'}), 403

            data = request.json
            user_id = data.get('id')
            nuevo_rol = data.get('rol')
            nuevos_tokens = data.get('tokens')

            if user_id is None:
                return jsonify({'message': 'Faltan datos'}), 400

            c.execute("UPDATE usuarios SET rol=?, tokens=? WHERE id=?", (nuevo_rol, nuevos_tokens, user_id))
            conn.commit()
            return jsonify({'message': 'Usuario actualizado'}), 200

    except Exception as e:
        print("⚠️ Error en /api/update-user:", e)
        return jsonify({'error': str(e)}), 500

# Log simulado
@app.route('/api/log', methods=['GET'])
def get_log():
    return jsonify({
        'log_lines': ['[00:00] Bot aún no ejecutado.'],
        'mensaje_bot': 'Esperando acción del usuario...'
    })

# Test
@app.route('/')
def home():
    return 'API funcionando correctamente 🔥 - MAAX 𒉭'

# Headers CORS
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

# Ejecutar
if __name__ == '__main__':
    app.run(debug=True)
