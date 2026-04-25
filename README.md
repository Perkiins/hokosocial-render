# HokoSocial — Backend (Render)

API Flask + SQLite del proyecto HokoSocial. Sirve auth (JWT), datos de usuario, ejecución de bot y panel admin.

## Stack
- Python 3.11+ / Flask + Gunicorn
- PyJWT, flask-cors, werkzeug.security
- SQLite (`usuarios.db`)
- Selenium + ChromeDriver (para el bot real)
- Deploy: Render (`Procfile: web: gunicorn followback_app:app`)

## Setup local

```bash
# con uv (recomendado)
uv venv
uv pip install -r requirements.txt
cp .env.example .env  # rellena SECRET_KEY y CORS_ORIGINS
python followback_app.py
```

```bash
# con pip clásico
python -m venv .venv && source .venv/Scripts/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
python followback_app.py
```

## Variables de entorno

| Variable | Requerida | Descripción |
|----------|-----------|-------------|
| `SECRET_KEY` | **sí en producción** | Clave para firmar JWT. Generar con `python -c "import secrets; print(secrets.token_urlsafe(48))"`. |
| `CORS_ORIGINS` | sí | Orígenes permitidos por CORS, coma-separados. Ej: `https://hokosocial.vercel.app` |
| `FLASK_DEBUG` | no | `true` sólo en local. Por defecto `false`. |
| `PORT` | no | Puerto local (Render inyecta su propio `PORT`). |
| `DB_PATH` | no | Ruta de la SQLite. Por defecto `usuarios.db`. |

## Endpoints

| Método | Path | Auth | Descripción |
|--------|------|------|-------------|
| `POST` | `/api/register` | público | Registro `{username, password}`. Devuelve `{token}`. |
| `POST` | `/api/login` | público | Login `{username, password}`. Rehashea passwords legacy en texto plano. |
| `GET` | `/api/user-data` | Bearer | Datos del usuario autenticado. |
| `POST` | `/api/run-bot` | Bearer | Decrementa 1 token y lanza tarea simulada. Devuelve `tokens_restantes`. |
| `GET` | `/api/log` | Bearer | Log de la última ejecución. `{log: [], mensaje_bot}` |
| `POST` | `/api/generar-cookies` | Bearer | Stub `501 Not Implemented`. |
| `GET` | `/api/admin/users` | Bearer + admin | Lista de usuarios. |
| `POST` | `/api/update-user` | Bearer + admin | Actualiza `tokens` y/o `rol`. |
| `DELETE` | `/api/admin/delete-user/<username>` | Bearer + admin | Elimina usuario (no permite auto-borrado). |

## Seguridad

- **SECRET_KEY**: nunca commitear. Configurar en Render como env var.
- **Passwords**: se almacenan hasheadas con `werkzeug.generate_password_hash` (PBKDF2-SHA256). Las passwords legacy en texto plano siguen funcionando y se rehashean en el siguiente login del usuario (compatibilidad).
- **CORS**: lista blanca por env var. Añadir dominios de preview de Vercel si los usas.
- **JWT**: 12 h de vigencia. Frontend redirige a `/login` al recibir 401.

## Deploy en Render

1. **Build command**: `pip install -r requirements.txt`
2. **Start command**: `gunicorn followback_app:app`
3. **Env vars** (Settings → Environment):
   - `SECRET_KEY` = (generada localmente, no la del repo)
   - `CORS_ORIGINS` = `https://hokosocial.vercel.app`
4. **Health check**: `GET /` debería devolver `🔥 HokoSocial API activa`.

> ⚠️ El filesystem de Render es efímero. `usuarios.db` y los `log_*.txt` se reinician en cada deploy. Para datos persistentes, montar un Render Disk en `/var/data` y apuntar `DB_PATH` ahí.

## Estructura

```
followback_app.py     # app Flask principal
install_chrome.py     # instala Chrome para Render (bot real)
requirements.txt      # dependencias Python
Procfile              # web: gunicorn followback_app:app
usuarios.db           # SQLite (ver nota de persistencia)
api/log/              # logs históricos del bot
```
