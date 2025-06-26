# install_chrome.py
import os
import stat
import urllib.request
import zipfile
import shutil

CHROME_URL = "https://storage.googleapis.com/chromium-browser-snapshots/Linux_x64/1228606/chrome-linux.zip"
CHROMEDRIVER_URL = "https://storage.googleapis.com/chromium-browser-snapshots/Linux_x64/1228606/chromedriver_linux64.zip"

os.makedirs("bin", exist_ok=True)

def download_and_extract(url, extract_to, binary_name):
    zip_path = f"{extract_to}/{binary_name}.zip"
    urllib.request.urlretrieve(url, zip_path)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    os.remove(zip_path)

print("⬇️ Descargando Chromium...")
download_and_extract(CHROME_URL, "bin", "chrome")
shutil.move("bin/chrome-linux/chrome", "bin/chrome")
shutil.rmtree("bin/chrome-linux")

print("⬇️ Descargando Chromedriver...")
download_and_extract(CHROMEDRIVER_URL, "bin", "chromedriver")
shutil.move("bin/chromedriver_linux64/chromedriver", "bin/chromedriver")
shutil.rmtree("bin/chromedriver_linux64")

os.chmod("bin/chrome", os.stat("bin/chrome").st_mode | stat.S_IEXEC)
os.chmod("bin/chromedriver", os.stat("bin/chromedriver").st_mode | stat.S_IEXEC)

print("✅ Instalación de Chromium completada")
