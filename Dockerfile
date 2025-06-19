FROM python:3.13-slim

# Instalamos dependencias para Chrome
RUN apt-get update && apt-get install -y \
    wget \
    gnupg2 \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libgtk-3-0 \
    libnss3 \
    libxshmfence1 \
    xdg-utils \
    --no-install-recommends

# Añadimos repositorio de Chrome y lo instalamos
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

# Copiamos requirements y instalamos Python deps
COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install -r requirements.txt

# Copiamos el código
COPY . /app

# Puerto del servicio
EXPOSE 5000

# Comando para arrancar la app
CMD ["python", "app.py"]
