# Usamos imagen base Python 3.13 slim
FROM python:3.13-slim

# Instalamos herramientas básicas y Chromium + ChromeDriver
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Variables para Chrome y ChromeDriver
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROME_DRIVER=/usr/bin/chromedriver

# Creamos directorio de trabajo
WORKDIR /app

# Copiamos requirements y los instalamos
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos todo el código
COPY . .

# Exponemos el puerto (por defecto flask 5000)
EXPOSE 5000

# Ejecutamos la app
CMD ["python", "app.py"]
