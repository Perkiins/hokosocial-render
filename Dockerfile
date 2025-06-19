FROM python:3.11-slim

# Instalar dependencias del sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        firefox-esr \
        wget \
        unzip \
        curl \
        ca-certificates \
        fonts-liberation \
        libgtk-3-0 \
        libdbus-glib-1-2 \
        libasound2 \
        libxt6 \
        libxrandr2 \
        libxss1 \
        libxcomposite1 \
        libxcursor1 \
        libxdamage1 \
        libxi6 \
        libnss3 \
        libx11-xcb1 \
        libxrender1 \
        libxext6 \
        libxfixes3 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Instalar geckodriver
RUN GECKODRIVER_VERSION=0.33.0 && \
    wget -q https://github.com/mozilla/geckodriver/releases/download/v$GECKODRIVER_VERSION/geckodriver-v$GECKODRIVER_VERSION-linux64.tar.gz && \
    tar -xzf geckodriver-v$GECKODRIVER_VERSION-linux64.tar.gz -C /usr/local/bin && \
    rm geckodriver-v$GECKODRIVER_VERSION-linux64.tar.gz

# Crear directorio de trabajo
WORKDIR /app

# Copiar requirements y código
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3", "threads_bot_ejecucion.py"]
