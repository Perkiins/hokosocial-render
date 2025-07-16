from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3
import jwt
import datetime
import os
import threading

# Configuración
app = Flask(__name__)
app.config['SECRET_KEY'] = 'clave-super-secreta'
CORS(app, origins=["https://hokosocial.vercel.app"], supports_credentials=True)

DB_PATH = 'usuarios.db'

# Crear tabla si no existe
def crear_tabla():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS usuarios (
            username TEXT PRIMARY KEY,
            password TEXT,
            tokens INTEGER DEFAULT 5,
            rol TEXT DEFAULT 'user'
        )''')

crear_tabla()

# --- JWT ---
def generar_token(username):
    payload = {
        'username': username,
        'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=12)
    }
    return jwt.encode(payload, app.config['SECRET_KEY'], algorithm="HS256")

def verificar_token(token):
    try:
        return jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
    except:
        return None

# --- Registro ---
@app.route('/api/register', methods=['POST'])
def register():
    data = request.json
    username = data.get('username')
    password = data.get('password')

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        try:
            cur.execute("INSERT INTO usuarios (username, password) VALUES (?, ?)", (username, password))
            conn.commit()
            token = generar_token(username)
            return jsonify({"token": token}), 201
        except sqlite3.IntegrityError:
            return jsonify({"message": "Usuario ya existe"}), 409

# --- Login ---
@app.route('/api/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM usuarios WHERE username = ? AND password = ?", (username, password))
        if cur.fetchone():
            token = generar_token(username)
            return jsonify({"token": token})
    return jsonify({"message": "Credenciales incorrectas"}), 401

# --- User Data (protegido) ---
@app.route('/api/user-data', methods=['GET'])
def user_data():
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    user = verificar_token(token)
    if not user:
        return jsonify({"message": "Token inválido"}), 401

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT tokens, rol FROM usuarios WHERE username = ?", (user["username"],))
        row = cur.fetchone()
        return jsonify({"username": user["username"], "tokens": row[0], "rol": row[1]})

# --- Ejecutar bot (simulado) ---
@app.route('/api/run-bot', methods=['POST'])
def run_bot():
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    user = verificar_token(token)
    if not user:
        return jsonify({"message": "Token inválido"}), 401

    username = user["username"]
    log_file = f"log_{username}.txt"

    def tarea():
        with open(log_file, "w", encoding="utf-8") as f:
            f.write("🔄 Ejecutando bot...\n")
            import time
            for i in range(3):
                f.write(f"⏳ Proceso {i+1}/3\n")
                time.sleep(1)
            f.write("✅ Bot finalizado\n")

        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("UPDATE usuarios SET tokens = tokens - 1 WHERE username = ? AND tokens > 0", (username,))
            conn.commit()

    threading.Thread(target=tarea).start()
    return jsonify({"message": "Bot lanzado correctamente"})

# --- Log (protegido) ---
@app.route('/api/log', methods=['GET'])
def get_log():
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    user = verificar_token(token)
    if not user:
        return jsonify({"message": "Token inválido"}), 401

    username = user["username"]
    log_file = f"log_{username}.txt"
    if not os.path.exists(log_file):
        return jsonify({"log": [], "mensaje_bot": "Sin log aún."})

    with open(log_file, "r", encoding="utf-8") as f:
        lines = f.readlines()
    return jsonify({
        "log": [l.strip() for l in lines if l.strip()],
        "mensaje_bot": lines[-1].strip() if lines else "Sin mensaje final"
    })

# Test
@app.route('/')
def home():
    return "🔥 HokoSocial API activa"

if __name__ == '__main__':
    app.run(debug=True)
