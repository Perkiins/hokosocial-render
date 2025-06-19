# Usamos una imagen base oficial de Python
FROM python:3.13-slim

# Instalamos dependencias para Chrome y ChromeDriver
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    curl \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libappindicator3-1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Instalamos Google Chrome
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

# Instalamos ChromeDriver - versión compatible con Chrome (ajusta la versión si necesitas)
ENV CHROME_DRIVER_VERSION=117.0.5938.92
RUN wget -O /tmp/chromedriver.zip https://chromedriver.storage.googleapis.com/${CHROME_DRIVER_VERSION}/chromedriver_linux64.zip && \
    unzip /tmp/chromedriver.zip -d /usr/local/bin/ && \
    rm /tmp/chromedriver.zip && \
    chmod +x /usr/local/bin/chromedriver

# Creamos directorio de la app
WORKDIR /app

# Copiamos los archivos de la app
COPY . /app

# Instalamos dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# Exponemos el puerto (ajusta si usas otro)
EXPOSE 10000

# Comando para correr la app
CMD ["gunicorn", "app:app", "-b", "0.0.0.0:10000"]
