from flask import Flask, request, jsonify
from flask_cors import CORS  # 💥 Importación que faltaba
import sqlite3
import jwt
import datetime

# Configuración
app = Flask(__name__)
app.config['SECRET_KEY'] = 'clave-secreta-segura'  # Cámbiala por algo real
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

DB_PATH = 'usuarios.db'

# Crear tabla si no existe
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

# Ruta de registro
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

# Ruta de login
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

        return jsonify({'token': token}), 200
    else:
        return jsonify({'message': 'Credenciales incorrectas'}), 401

# Ruta protegida (requiere token)
@app.route('/api/user-data', methods=['GET'])
def user_data():
    token = request.headers.get('Authorization')
    if not token:
        return jsonify({'message': 'Token requerido'}), 401

    try:
        decoded = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
        return jsonify({'message': f'Datos del usuario {decoded["username"]}'}), 200
    except jwt.ExpiredSignatureError:
        return jsonify({'message': 'Token expirado'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'message': 'Token inválido'}), 401

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

# Test de vida
@app.route('/')
def home():
    return 'API funcionando correctamente 🔥 - MAAX 𒉭'

if __name__ == '__main__':
    app.run(debug=True)
