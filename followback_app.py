# followback_app.py - Flask App con Cookies y SQLite en Render

import os
import json
import sqlite3
import threading
import time
import random
from flask import Flask, render_template, request, redirect, url_for, flash, session
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# --- CONFIGURACIÓN FLASK ---
app = Flask(__name__)
app.secret_key = 'clave-secreta'

# --- BASE DE DATOS ---
DB_FILE = 'usuarios.db'
def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password TEXT,
            tokens INTEGER DEFAULT 5
        )''')

# --- FUNCIONES COOKIES ---
def guardar_cookies(driver, username):
    cookies = driver.get_cookies()
    with open(f"cookies_{username}.json", "w", encoding='utf-8') as f:
        json.dump(cookies, f)

def cargar_cookies(driver, username):
    try:
        path = f"cookies_{username}.json"
        with open(path, "r", encoding='utf-8') as f:
            cookies = json.load(f)
            for cookie in cookies:
                cookie_clean = {
                    "name": cookie.get("name"),
                    "value": cookie.get("value"),
                    "domain": cookie.get("domain"),
                    "path": cookie.get("path", "/"),
                    "secure": cookie.get("secure", False),
                    "httpOnly": cookie.get("httpOnly", False),
                }
                if "expirationDate" in cookie:
                    cookie_clean["expiry"] = int(cookie["expirationDate"])
                driver.add_cookie(cookie_clean)
        return True
    except Exception as e:
        print(f"⛔ Error cargando cookies desde {path}: {e}")
        return False

# --- FUNCIONES DEL BOT ---
def ejecutar_bot(username, log_fn=print):
    log_fn(f"🟢 Iniciando ejecución del bot para: {username}")

    CHROME_BIN = "/opt/render/project/src/bin/chrome"
    CHROMEDRIVER_BIN = "/opt/render/project/src/bin/chromedriver"

    chrome_options = Options()
    chrome_options.binary_location = CHROME_BIN
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--remote-debugging-port=9222")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument("--window-size=1920,1080")

    try:
        driver = webdriver.Chrome(service=ChromeService(CHROMEDRIVER_BIN), options=chrome_options)
        driver.get("https://www.threads.net")
        log_fn("✅ Chrome iniciado y Threads abierto")

        if not cargar_cookies(driver, username):
            log_fn("⛔ No se cargaron las cookies")
            driver.quit()
            return False, "No hay cookies. Debes iniciarlas desde el panel."

        log_fn("✅ Cookies cargadas correctamente")
        archivo_seguidos = f"seguidos_{username}.txt"
        seguidos = set()
        if os.path.exists(archivo_seguidos):
            with open(archivo_seguidos, "r", encoding="utf-8") as f:
                seguidos = set(line.strip() for line in f)

        nuevos_seguidos = 0
        log_fn("🔍 Buscando posibles perfiles...")
        perfiles = buscar_perfiles_en_para_ti(driver, 50, log_fn)
        log_fn(f"✅ Encontrados {len(perfiles)} perfiles")
        random.shuffle(perfiles)

        log_fn("🔎 Analizando perfiles...")
        total = len(perfiles)
        for i, usuario in enumerate(perfiles):
            if usuario in seguidos:
                continue
            if analizar_perfil(driver, usuario):
                seguir_usuario(driver, usuario, seguidos)
                nuevos_seguidos += 1
                with open(archivo_seguidos, "a", encoding="utf-8") as f:
                    f.write(usuario + "\n")
                log_fn(f"👍 Seguido {nuevos_seguidos}/{total}: @{usuario}")
            time.sleep(random.uniform(3, 5))

        driver.quit()
        log_fn(f"✅ Bot finalizado. Nuevos seguidos: {nuevos_seguidos}")
        return True, f"Se siguieron {nuevos_seguidos} perfiles nuevos."

    except Exception as e:
        log_fn(f"⛔ Error durante la ejecución del bot: {e}")
        return False, "Error durante la ejecución. Mira la consola."

def buscar_perfiles_en_para_ti(driver, cantidad_a_extraer=300, log_fn=print):
    log_fn("🌐 Navegando por la sección 'Para ti'")
    try:
        driver.get("https://www.threads.net/")
        usuarios_encontrados = set()
        intentos_scroll = 0
        max_intentos_scroll = 50

        while len(usuarios_encontrados) < cantidad_a_extraer and intentos_scroll < max_intentos_scroll:
            for _ in range(3):
                driver.execute_script("window.scrollBy(0, window.innerHeight * 2);")
                time.sleep(1)

            autores_elements = driver.find_elements(By.XPATH, '//a[contains(@href, "/@") and not(contains(@href, "/post/")) and contains(@class, "x1i10hfl") and contains(@class, "xjbqb8w")]')
            for elemento in autores_elements:
                href = elemento.get_attribute('href')
                if href and "/@" in href:
                    username = href.split('/@')[1].split("/")[0]
                    if username.lower() == "maaxperkiins":
                        continue
                    usuarios_encontrados.add(username)
                    if len(usuarios_encontrados) >= cantidad_a_extraer:
                        break

            intentos_scroll += 1

        log_fn(f"📦 Se extrajeron {len(usuarios_encontrados)} usuarios.")
        return list(usuarios_encontrados)

    except Exception as e:
        log_fn(f"⛔ Error buscando perfiles: {e}")
        return []

# --- ESPERAS ---  
def esperar_splash_desaparecer(driver, timeout=15):
    try:
        WebDriverWait(driver, timeout).until(
            EC.invisibility_of_element_located((By.ID, "barcelona-splash-screen"))
        )
    except TimeoutException:
        pass

def esperar_elemento(driver, by, selector, timeout=10):
    try:
        return WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((by, selector))
        )
    except:
        return None
    
# --- FUNCIONES BOT ---
def analizar_perfil(driver, username, log_fn=print):
    url_perfil = f"https://www.threads.net/@{username}"
    print(f"Analizando el perfil de: {username}")
    try:
        driver.get(url_perfil)
        time.sleep(random.uniform(2, 4))

        seguidores_popup = 0
        siguiendo_popup = 0

        seguidores_button = esperar_elemento(driver, By.XPATH, '//div[@class="x78zum5 x2lah0s"]/div[@role="button" and @tabindex="0"]/div/span[contains(text(), "seguidores")]/ancestor::div[@role="button"]')
        if seguidores_button:
            try:
                driver.execute_script("arguments[0].click();", seguidores_button)
                WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, '//div[@role="dialog"]')))
                time.sleep(random.uniform(2, 3))
            except Exception as e:
                print(f"Error al hacer clic en 'Seguidores': {e}")
                return False

            seguidores_numero_element = esperar_elemento(driver, By.XPATH, '//div[@role="tab" and .//div[@aria-label="Seguidores"]]//span[@title]', timeout=15)
            if seguidores_numero_element:
                seguidores_title = seguidores_numero_element.get_attribute('title')
                print(f"Texto del title (seguidores) de {username}: '{seguidores_title}'")
                try:
                    seguidores_text = seguidores_title.replace('\xa0', '').replace(' ', '').replace('.', '').replace(',', '.')
                    seguidores_popup = int(float(seguidores_text))
                except ValueError as e:
                    print(f"No se pudo parsear el número de seguidores: {seguidores_text} - Error: {e}")

            siguiendo_numero_element = esperar_elemento(driver, By.XPATH, '//div[@role="tab" and .//div[@aria-label="Siguiendo"]]//span[@title]', timeout=15)
            if siguiendo_numero_element:
                siguiendo_title = siguiendo_numero_element.get_attribute('title')
                print(f"Texto del title (siguiendo) de {username}: '{siguiendo_title}'")
                try:
                    siguiendo_text = siguiendo_title.replace('\xa0', ' ').replace('.', '').replace(',', '.')
                    siguiendo_popup = int(float(siguiendo_text))
                except ValueError as e:
                    print(f"No se pudo parsear el número de siguiendo: {siguiendo_text} - Error: {e}")

            # ✅ NUEVA LÓGICA DE DIFERENCIA PERCENTUAL
            if seguidores_popup >= 20 and siguiendo_popup >= 20 and seguidores_popup > 0:
                if siguiendo_popup > seguidores_popup:
                    print(f"✅ El perfil de {username} tiene más seguidos que seguidores. Se seguirá sin importar diferencia.")
                    return True
                else:
                    diferencia = abs(siguiendo_popup - seguidores_popup) / seguidores_popup
                    print(f"Diferencia porcentual entre siguiendo y seguidores: {diferencia:.2%}")
                    if 0 <= diferencia <= 0.30:
                        print(f"✅ El perfil de {username} cumple el criterio. Se seguirá.")
                        return True
                    else:
                        print(f"⛔ El perfil de {username} NO cumple la diferencia requerida.")
                        return False
            elif seguidores_popup <= 0:
                print(f"⛔ El perfil tiene cero o menos seguidores.")
                return False
            else:
                print(f"⛔ No tiene suficientes seguidores o seguidos.")
                return False

    except Exception as e:
        print(f"Error general al analizar el perfil de {username}: {e}")
        return False

def seguir_usuario(driver, username, lista_seguidos):
    if username not in lista_seguidos:
        url_perfil = f"https://www.threads.net/@{username}"
        print(f"Intentando seguir a: {username}")
        try:
            driver.get(url_perfil)
            esperar_splash_desaparecer(driver)

            boton_seguir = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, '//div[@role="button" and .//div[text()="Seguir"]]'))
            )
            boton_seguir.click()
            lista_seguidos.add(username)
            log_fn(f"👍 Se ha seguido a: {username}")
            time.sleep(random.uniform(5, 10))

        except TimeoutException:
            print(f"⛔ No se encontró el botón 'Seguir' en {username} o el splash no desapareció.")
        except Exception as e:
            print(f"Error al seguir a {username}: {e}")

        time.sleep(random.uniform(3, 6))
    else:
        print(f"Ya se ha intentado seguir a: {username}")


# --- RUTAS FLASK ---
@app.route('/')
def index():
    if 'username' in session:
        return redirect(url_for('panel'))
    return redirect(url_for('login'))

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM users WHERE username = ?", (username,))
            if cur.fetchone():
                flash("Usuario ya existe", "error")
            else:
                cur.execute("INSERT INTO users (username, password) VALUES (?, ?)", (username, password))
                conn.commit()
                flash("Usuario registrado", "success")
                return redirect(url_for('login'))
    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM users WHERE username = ? AND password = ?", (username, password))
            user = cur.fetchone()
            if user:
                session['username'] = username
                return redirect(url_for('panel'))
            else:
                flash("Usuario o contraseña incorrectos", "error")
    return render_template('login.html')

@app.route('/panel')
def panel():
    if 'username' not in session:
        return redirect(url_for('login'))

    username = session['username']

    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute("SELECT tokens FROM users WHERE username = ?", (username,))
        tokens = cur.fetchone()[0]

    # Leer mensaje de estado final del bot
    msg = ""
    path_msg = f"msg_{username}.txt"
    if os.path.exists(path_msg):
        with open(path_msg, "r", encoding='utf-8') as f:
            msg = f.read()
        os.remove(path_msg)

    # Leer log estilo consola
    log_lines = []
    log_file = f"log_{username}.txt"
    if os.path.exists(log_file):
        with open(log_file, "r", encoding="utf-8") as f:
            log_lines = f.readlines()
        log_lines = [l.strip() for l in log_lines if l.strip()]

    return render_template('panel.html', tokens=tokens, mensaje_bot=msg, log_lines=log_lines)

def append_log(username, text):
    with open(f"log_{username}.txt", "a", encoding="utf-8") as f:
        f.write(text + "\n")
        
@app.route('/run_bot', methods=['POST'])
def run_bot():
    if 'username' not in session:
        return redirect(url_for('login'))

    username = session['username']

    def run_async(username):
        # Limpiar log anterior
        log_file = f"log_{username}.txt"
        open(log_file, "w", encoding="utf-8").close()

        append_log(username, "🔄 Bot en ejecución...")

        success, msg = ejecutar_bot(username, log_fn=lambda t: append_log(username, t))

        with sqlite3.connect(DB_FILE) as conn:
            if success:
                conn.execute("UPDATE users SET tokens = tokens - 1 WHERE username = ? AND tokens > 0", (username,))
            with open(f"msg_{username}.txt", "w", encoding='utf-8') as f:
                f.write(msg)

        append_log(username, f"🔁 Resultado final: {msg}")
        print(f"🔁 Resultado: {msg}")

    threading.Thread(target=run_async, args=(username,)).start()
    flash("Bot en ejecución...", "info")
    return redirect(url_for('panel'))

@app.route('/generar_cookies', methods=['POST'])
def generar_cookies():
    if 'username' not in session:
        return redirect(url_for('login'))

    username = session['username']
    print(f"🔐 Iniciando Chrome para login manual de: {username}")

    chrome_options = Options()
    chrome_options.add_experimental_option("detach", True)  # No cerrar automáticamente

    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    driver.get("https://www.threads.net")

    input("🔵 Inicia sesión manualmente y pulsa Enter aquí para guardar cookies...")

    guardar_cookies(driver, username)
    driver.quit()
    flash("Cookies guardadas correctamente. Ya puedes ejecutar el bot.", "success")
    return redirect(url_for('panel'))

@app.route('/logout')
def logout():
    session.pop('username', None)
    flash("Has cerrado sesión", "info")
    return redirect(url_for('login'))

if __name__ == '__main__':
    init_db()
    app.run(debug=True)
