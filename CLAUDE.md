# HokoSocial — Backend (Render)

Backend del proyecto HokoSocial. Bot de followback con automatización via Selenium.

## Stack
- **Runtime**: Python
- **Framework**: Flask + Gunicorn
- **Auth**: PyJWT
- **Automatización**: Selenium + ChromeDriver (chromedriver-autoinstaller)
- **Base de datos**: SQLite (`usuarios.db`)
- **Deploy**: Render (Procfile: `web: gunicorn followback_app:app`)

## Archivos
- `followback_app.py` — app principal Flask + lógica del bot
- `install_chrome.py` — instalación de Chrome para Render
- `requirements.txt` — dependencias Python
- `Procfile` — configuración de inicio en Render
- `usuarios.db` — base de datos SQLite local
- `api/log/` — logs de la API

## Comandos
```bash
pip install -r requirements.txt   # setup
python followback_app.py          # desarrollo local
```

## Proyecto relacionado
El frontend es **hoko-social** (`E:\Maax\hoko-social`) — React/Vite desplegado en Vercel.
