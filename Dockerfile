# Usa imagen base oficial de Python slim
FROM python:3.10-slim

# Evitar warnings sobre stdin no TTY
ENV DEBIAN_FRONTEND=noninteractive

# Actualiza e instala Firefox ESR y Geckodriver, y dependencias para Selenium y headless
RUN apt-get update && apt-get install -y \
    firefox-esr \
    wget \
    curl \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# Descarga Geckodriver y lo instala
ENV GECKODRIVER_VERSION=v0.33.0
RUN wget -q "https://github.com/mozilla/geckodriver/releases/download/${GECKODRIVER_VERSION}/geckodriver-${GECKODRIVER_VERSION}-linux64.tar.gz" \
    && tar -xzf geckodriver-${GECKODRIVER_VERSION}-linux64.tar.gz -C /usr/local/bin/ \
    && rm geckodriver-${GECKODRIVER_VERSION}-linux64.tar.gz \
    && chmod +x /usr/local/bin/geckodriver

# Crea carpeta de trabajo
WORKDIR /app

# Copia requirements.txt e instala
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo el código
COPY . .

# Expone puerto 8080 para Flask
EXPOSE 8080

# Comando para correr la app Flask
CMD ["python3", "app.py"]
