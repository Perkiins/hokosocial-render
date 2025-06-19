import logging
from logging.handlers import RotatingFileHandler
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
import time
import random
import os

# --- CONFIGURACIÓN DEL LOGGER ---
LOG_FILE = "threads_bot.log"
logger = logging.getLogger("threads_bot")
logger.setLevel(logging.DEBUG)

file_handler = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=5, encoding="utf-8")
file_handler.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)
# -------------------------------

lista_seguidos = set()

def esperar_splash_desaparecer(driver, timeout=15):
    try:
        WebDriverWait(driver, timeout).until(
            EC.invisibility_of_element_located((By.ID, "barcelona-splash-screen"))
        )
        logger.debug("Splash screen desapareció.")
    except TimeoutException:
        logger.warning("Timeout esperando que el splash screen desaparezca.")

def esperar_elemento(driver, by, selector, timeout=10):
    try:
        return WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((by, selector))
        )
    except Exception as e:
        logger.debug(f"Error al esperar el elemento '{selector}': {e}")
        return None

def analizar_perfil(driver, username):
    try:
        url = f"https://www.threads.net/@{username}"
        driver.get(url)
        time.sleep(random.uniform(2, 4))

        seguidores_button = esperar_elemento(driver, By.XPATH, '//div[@class="x78zum5 x2lah0s"]/div[@role="button" and @tabindex="0"]/div/span[contains(text(), "seguidores")]/ancestor::div[@role="button"]')
        if not seguidores_button:
            logger.info(f"No se encontró botón seguidores en {username}")
            return False

        driver.execute_script("arguments[0].click();", seguidores_button)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, '//div[@role="dialog"]')))
        time.sleep(random.uniform(2, 3))

        seguidores = esperar_elemento(driver, By.XPATH, '//div[@role="tab" and .//div[@aria-label="Seguidores"]]//span[@title]')
        siguiendo = esperar_elemento(driver, By.XPATH, '//div[@role="tab" and .//div[@aria-label="Siguiendo"]]//span[@title]')

        if not seguidores or not siguiendo:
            logger.info(f"No se encontró número de seguidores o siguiendo en {username}")
            return False

        seguidores_num = int(float(seguidores.get_attribute('title').replace('\xa0', ' ').replace('.', '').replace(',', '.')))
        siguiendo_num = int(float(siguiendo.get_attribute('title').replace('\xa0', ' ').replace('.', '').replace(',', '.')))

        if seguidores_num <= 0 or siguiendo_num <= 0:
            logger.info(f"Perfil {username} tiene 0 seguidores o siguiendo.")
            return False

        if siguiendo_num > seguidores_num:
            logger.info(f"Perfil {username} tiene más seguidos que seguidores. Cumple criterio.")
            return True

        diferencia = abs(siguiendo_num - seguidores_num) / seguidores_num
        cumple = diferencia <= 0.30
        logger.info(f"Diferencia porcentual en {username}: {diferencia:.2%}. Cumple: {cumple}")
        return cumple

    except Exception as e:
        logger.error(f"Error analizando perfil {username}: {e}")
        return False

def seguir_usuario(driver, username):
    if username in lista_seguidos:
        logger.info(f"Ya se intentó seguir a: {username}")
        return False

    url_perfil = f"https://www.threads.net/@{username}"
    logger.info(f"Intentando seguir a: {username}")
    try:
        driver.get(url_perfil)
        esperar_splash_desaparecer(driver)

        boton_seguir = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, '//div[@role="button" and .//div[text()="Seguir"]]'))
        )
        boton_seguir.click()
        lista_seguidos.add(username)
        logger.info(f"👍 Se ha seguido a: {username}")

        time.sleep(random.uniform(5, 8))
        driver.get("https://www.threads.net/")
        time.sleep(random.uniform(3, 6))
        return True

    except TimeoutException:
        logger.warning(f"No se encontró botón 'Seguir' o splash no desapareció en {username}.")
    except ElementClickInterceptedException as e:
        logger.warning(f"Clic interceptado en {username}: {e}")
    except Exception as e:
        logger.error(f"Error al seguir a {username}: {e}")
    return False

def buscar_perfiles(driver, cantidad=100):
    try:
        driver.get("https://www.threads.net/")
        encontrados = set()
        intentos = 0

        while len(encontrados) < cantidad and intentos < 30:
            driver.execute_script("window.scrollBy(0, window.innerHeight * 2);")
            time.sleep(1)

            elementos = driver.find_elements(By.XPATH, '//a[contains(@href, "/@") and not(contains(@href, "/post/")) and contains(@class, "x1i10hfl") and contains(@class, "xjbqb8w")]')
            for el in elementos:
                href = el.get_attribute("href")
                if href and "/@" in href:
                    user = href.split("/@")[1].split("/")[0]
                    if user.lower() != "maaxperkiins":
                        encontrados.add(user)
                if len(encontrados) >= cantidad:
                    break

            intentos += 1
        logger.info(f"Se encontraron {len(encontrados)} perfiles en 'Para ti'.")
        return list(encontrados)
    except Exception as e:
        logger.error(f"Error buscando perfiles: {e}")
        return []

def ejecutar_bot():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    service = FirefoxService()  # Usa el geckodriver en PATH (instalado en Dockerfile)
    driver = webdriver.Firefox(service=service, options=options)

    if os.path.exists("seguidos.txt"):
        with open("seguidos.txt", "r", encoding="utf-8") as f:
            for linea in f:
                lista_seguidos.add(linea.strip())
        logger.info(f"Cargados {len(lista_seguidos)} usuarios ya seguidos desde archivo.")

    try:
        while True:
            candidatos = buscar_perfiles(driver, cantidad=60)
            random.shuffle(candidatos)

            for user in candidatos:
                if user in lista_seguidos:
                    continue
                if analizar_perfil(driver, user):
                    if seguir_usuario(driver, user):
                        with open("seguidos.txt", "a", encoding="utf-8") as f:
                            f.write(user + "\n")
                        logger.info(f"Guardado {user} en seguidos.txt")
                    else:
                        logger.info(f"No se pudo seguir a {user}")

                time.sleep(random.uniform(3, 6))

            logger.info("🔁 Volviendo a buscar más perfiles en 'Para ti'...\n")
            time.sleep(random.uniform(10, 20))

    except KeyboardInterrupt:
        logger.info("🛑 Script detenido manualmente por el usuario.")
    except Exception as e:
        logger.error(f"Error en ejecución principal: {e}")
    finally:
        driver.quit()
        logger.info("Driver cerrado y script finalizado.")

if __name__ == "__main__":
    logger.info("Iniciando el script...")
    ejecutar_bot()
