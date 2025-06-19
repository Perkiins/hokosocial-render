from flask import Flask, jsonify
import threading
import time
import logging
import threads_bot_ejecucion

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

bot_thread = None

def run_bot_periodically():
    while True:
        success, msg = threads_bot_ejecucion.ejecutar_bot_una_vez()
        logging.info(f"Bot run: {msg}")
        time.sleep(60 * 30)  # Corre cada 30 minutos

@app.route("/")
def home():
    return "ThreadsFollower bot activo."

@app.route("/healthz")
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    bot_thread = threading.Thread(target=run_bot_periodically, daemon=True)
    bot_thread.start()
    app.run(host="0.0.0.0", port=8080)
