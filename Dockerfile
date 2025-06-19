FROM python:3.11-slim

# Instalar dependencias para Firefox y geckodriver
RUN apt-get update && apt-get install -y \
    firefox-esr \
    wget \
    libgtk-3-0 \
    libdbus-glib-1-2 \
    libxt6 \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

# Instalar geckodriver
RUN GECKODRIVER_VERSION=0.33.0 \
    && wget -q "https://github.com/mozilla/geckodriver/releases/download/v${GECKODRIVER_VERSION}/geckodriver-v${GECKODRIVER_VERSION}-linux64.tar.gz" -O /tmp/geckodriver.tar.gz \
    && tar -xzf /tmp/geckodriver.tar.gz -C /usr/local/bin/ \
    && chmod +x /usr/local/bin/geckodriver \
    && rm /tmp/geckodriver.tar.gz

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5000

# Para que el bot funcione y el server también, usa supervisord o ejecuta solo el bot si solo quieres el bot:
CMD ["python3", "app.py"]
