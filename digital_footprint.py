"""HokoSocial — Digital Footprint scanner.

Escanea la presencia del PROPIO usuario (consentimiento explícito) en:
  - Apps de citas (Bumble, Tinder, Hinge, Badoo, OkCupid, POF, Meetic, Happn)
  - Filtraciones de datos (HIBP)
  - Plataformas de huella (Spotify, Reddit, GitHub, Strava, Steam)
  - Foto (FaceCheck.id, opcional)

Cada probe es una función independiente. Devuelve `ProbeResult`. El orquestador
los lanza en paralelo con timeout y los agrupa por categoría.

⚠️ Endpoints de apps de citas son frágiles — cambian sin avisar. Cuando un probe
falla por Cloudflare o respuesta inesperada, devuelve confidence='none' y NO
inventa un veredicto. Mejor "no determinable" que falso positivo/negativo.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import os
import random
import re
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from typing import Callable, Optional

import requests

log = logging.getLogger('hokosocial.footprint')

UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'
)

# Categorías
CAT_DATING = 'dating'
CAT_BREACH = 'breach'
CAT_PLATFORM = 'platform'
CAT_FACE = 'face'

# Confianza
CONF_HIGH = 'high'
CONF_MEDIUM = 'medium'
CONF_LOW = 'low'
CONF_NONE = 'none'


@dataclass
class ProbeResult:
    service: str
    category: str
    found: Optional[bool]  # True / False / None (no determinable)
    confidence: str
    detail: str = ''
    extra: dict = field(default_factory=dict)
    duration_ms: int = 0
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


# --- Helpers ---

def _hash_input(value: str) -> str:
    return hashlib.sha256((value or '').strip().lower().encode('utf-8')).hexdigest()


def _normalize_phone(phone: str) -> str:
    """Devuelve el teléfono en formato E.164 limpio (solo dígitos + opcional '+')."""
    if not phone:
        return ''
    cleaned = re.sub(r'[^\d+]', '', phone.strip())
    if not cleaned:
        return ''
    if not cleaned.startswith('+'):
        cleaned = '+' + cleaned
    return cleaned


def _normalize_email(email: str) -> str:
    return (email or '').strip().lower()


def _mask_email(email: str) -> str:
    if not email or '@' not in email:
        return ''
    local, _, domain = email.partition('@')
    return (local[:2] + '***') if len(local) > 2 else '***' + '@' + domain


def _mask_phone(phone: str) -> str:
    if not phone:
        return ''
    digits_only = re.sub(r'\D', '', phone)
    if len(digits_only) <= 4:
        return '***' + digits_only
    return '***' + digits_only[-4:]


def _is_cloudflare_block(text: str, status: int) -> bool:
    if status in (403, 503, 429):
        snippet = (text or '')[:2000].lower()
        return 'cloudflare' in snippet or 'just a moment' in snippet or 'attention required' in snippet
    return False


def _safe_jitter(base: float = 0.2, jitter: float = 0.6) -> None:
    time.sleep(base + random.random() * jitter)


def _measure(fn: Callable[..., ProbeResult], *args, **kwargs) -> ProbeResult:
    t0 = time.monotonic()
    try:
        result = fn(*args, **kwargs)
    except requests.Timeout:
        return ProbeResult(
            service=getattr(fn, '_service_name', fn.__name__),
            category=getattr(fn, '_service_cat', 'platform'),
            found=None, confidence=CONF_NONE,
            detail='Timeout', error='timeout',
            duration_ms=int((time.monotonic() - t0) * 1000),
        )
    except Exception as e:
        log.exception('probe %s crashed', fn.__name__)
        return ProbeResult(
            service=getattr(fn, '_service_name', fn.__name__),
            category=getattr(fn, '_service_cat', 'platform'),
            found=None, confidence=CONF_NONE,
            detail='Error interno', error=str(e)[:200],
            duration_ms=int((time.monotonic() - t0) * 1000),
        )
    result.duration_ms = int((time.monotonic() - t0) * 1000)
    return result


def _service(name: str, category: str):
    """Decorator: anota el probe con su nombre/categoría para errores."""
    def deco(fn):
        fn._service_name = name
        fn._service_cat = category
        return fn
    return deco


# --- Probes: Filtraciones ---

@_service('hibp', CAT_BREACH)
def probe_hibp(email: str | None, phone: str | None = None) -> ProbeResult:
    if not email:
        return ProbeResult('hibp', CAT_BREACH, None, CONF_NONE, 'Sin email')
    api_key = os.environ.get('HIBP_API_KEY', '').strip()
    if not api_key:
        return ProbeResult('hibp', CAT_BREACH, None, CONF_NONE,
                           'HIBP_API_KEY no configurada — la búsqueda de filtraciones está deshabilitada')
    url = f'https://haveibeenpwned.com/api/v3/breachedaccount/{urllib.parse.quote(email)}?truncateResponse=false'
    r = requests.get(url, headers={
        'hibp-api-key': api_key,
        'User-Agent': 'HokoSocial-DigitalFootprint',
    }, timeout=20)
    if r.status_code == 404:
        return ProbeResult('hibp', CAT_BREACH, False, CONF_HIGH,
                           'No apareces en filtraciones conocidas')
    if r.status_code == 200:
        breaches = r.json() or []
        summary = [{
            'name': b.get('Name'),
            'title': b.get('Title'),
            'date': b.get('BreachDate'),
            'pwn_count': b.get('PwnCount'),
            'data_classes': b.get('DataClasses') or [],
            'is_sensitive': bool(b.get('IsSensitive')),
        } for b in breaches]
        return ProbeResult('hibp', CAT_BREACH, True, CONF_HIGH,
                           f'{len(breaches)} filtraciones detectadas',
                           extra={'breaches': summary})
    if r.status_code == 401:
        return ProbeResult('hibp', CAT_BREACH, None, CONF_NONE,
                           'API key de HIBP inválida.')
    if r.status_code == 429:
        return ProbeResult('hibp', CAT_BREACH, None, CONF_NONE,
                           'HIBP nos rate-limiteó. Reintenta en unos segundos.')
    return ProbeResult('hibp', CAT_BREACH, None, CONF_LOW,
                       f'HIBP respondió HTTP {r.status_code}')


# --- Probes: Plataformas de huella general ---

@_service('github', CAT_PLATFORM)
def probe_github(email: str | None, phone: str | None = None) -> ProbeResult:
    """GitHub no expone búsqueda por email vía API pública para users de pago.
    Usamos search por commit-email que sí encuentra usuarios que han commiteado
    con ese email (mucha gente).
    """
    if not email:
        return ProbeResult('github', CAT_PLATFORM, None, CONF_NONE, 'Sin email')
    url = 'https://api.github.com/search/users'
    r = requests.get(url, params={'q': f'{email} in:email'},
                     headers={'User-Agent': UA, 'Accept': 'application/vnd.github+json'},
                     timeout=10)
    if r.status_code == 200:
        data = r.json() or {}
        total = data.get('total_count', 0)
        items = (data.get('items') or [])[:5]
        if total > 0:
            return ProbeResult('github', CAT_PLATFORM, True, CONF_MEDIUM,
                               f'Cuenta de GitHub que ha commiteado con este email',
                               extra={'usernames': [u.get('login') for u in items]})
        return ProbeResult('github', CAT_PLATFORM, False, CONF_LOW,
                           'Sin commits públicos con este email — puede tener cuenta sin commits públicos')
    if r.status_code == 403:
        return ProbeResult('github', CAT_PLATFORM, None, CONF_NONE,
                           'GitHub rate-limit. Reintenta más tarde.')
    return ProbeResult('github', CAT_PLATFORM, None, CONF_NONE,
                       f'GitHub HTTP {r.status_code}')


@_service('spotify', CAT_PLATFORM)
def probe_spotify(email: str | None, phone: str | None = None) -> ProbeResult:
    """Spotify expone validate=1 en su endpoint de signup."""
    if not email:
        return ProbeResult('spotify', CAT_PLATFORM, None, CONF_NONE, 'Sin email')
    url = 'https://spclient.wg.spotify.com/signup/public/v1/account'
    r = requests.get(url, params={'validate': '1', 'email': email},
                     headers={'User-Agent': UA, 'Accept': 'application/json'},
                     timeout=10)
    if r.status_code != 200:
        return ProbeResult('spotify', CAT_PLATFORM, None, CONF_LOW,
                           f'Spotify HTTP {r.status_code}')
    try:
        data = r.json()
    except Exception:
        return ProbeResult('spotify', CAT_PLATFORM, None, CONF_NONE,
                           'Spotify respondió no-JSON')
    # status=1: válido (libre). status=20: ya en uso. errors.email presente: ya existe.
    status = data.get('status')
    has_email_err = bool((data.get('errors') or {}).get('email'))
    if status == 20 or has_email_err:
        return ProbeResult('spotify', CAT_PLATFORM, True, CONF_HIGH,
                           'Cuenta de Spotify activa con este email')
    if status == 1:
        return ProbeResult('spotify', CAT_PLATFORM, False, CONF_HIGH,
                           'No hay cuenta de Spotify con este email')
    return ProbeResult('spotify', CAT_PLATFORM, None, CONF_LOW,
                       f'Respuesta de Spotify ambigua (status={status})')


@_service('strava', CAT_PLATFORM)
def probe_strava(email: str | None, phone: str | None = None) -> ProbeResult:
    """Strava expone si el email está registrado al intentar signup."""
    if not email:
        return ProbeResult('strava', CAT_PLATFORM, None, CONF_NONE, 'Sin email')
    # Best-effort: request al endpoint público
    url = 'https://www.strava.com/email_unique'
    r = requests.get(url, params={'email': email},
                     headers={'User-Agent': UA, 'Accept': 'application/json'},
                     timeout=10)
    if r.status_code == 200:
        try:
            data = r.json()
        except Exception:
            return ProbeResult('strava', CAT_PLATFORM, None, CONF_LOW, 'Respuesta no-JSON')
        # API legacy devolvía {"unique": true/false}
        unique = data.get('unique')
        if unique is True:
            return ProbeResult('strava', CAT_PLATFORM, False, CONF_HIGH, 'No hay cuenta de Strava')
        if unique is False:
            return ProbeResult('strava', CAT_PLATFORM, True, CONF_HIGH, 'Cuenta de Strava activa')
    return ProbeResult('strava', CAT_PLATFORM, None, CONF_LOW,
                       f'Strava HTTP {r.status_code}')


@_service('reddit', CAT_PLATFORM)
def probe_reddit(email: str | None, phone: str | None = None) -> ProbeResult:
    """Reddit no enumera por email. Si se nos pasa un username (parte local del
    email) podemos comprobar si EXISTE como usuario público."""
    if not email or '@' not in email:
        return ProbeResult('reddit', CAT_PLATFORM, None, CONF_NONE, 'Sin email')
    username = email.split('@', 1)[0]
    if not re.match(r'^[a-zA-Z0-9_-]{3,20}$', username):
        return ProbeResult('reddit', CAT_PLATFORM, None, CONF_NONE,
                           'La parte local del email no es un handle válido en Reddit')
    url = f'https://www.reddit.com/user/{username}/about.json'
    r = requests.get(url, headers={'User-Agent': UA}, timeout=10)
    if r.status_code == 200:
        try:
            data = r.json() or {}
        except Exception:
            data = {}
        if data.get('kind') == 't2':
            return ProbeResult('reddit', CAT_PLATFORM, True, CONF_MEDIUM,
                               f'Existe usuario público u/{username} (mismo handle que tu email)',
                               extra={'username': username})
    if r.status_code == 404:
        return ProbeResult('reddit', CAT_PLATFORM, False, CONF_MEDIUM,
                           f'No existe u/{username} (probado por handle del email)')
    return ProbeResult('reddit', CAT_PLATFORM, None, CONF_LOW,
                       f'Reddit HTTP {r.status_code}')


@_service('steam', CAT_PLATFORM)
def probe_steam(email: str | None, phone: str | None = None) -> ProbeResult:
    """Steam no enumera por email. Probamos por handle del email."""
    if not email or '@' not in email:
        return ProbeResult('steam', CAT_PLATFORM, None, CONF_NONE, 'Sin email')
    username = email.split('@', 1)[0]
    if not re.match(r'^[a-zA-Z0-9_-]{3,32}$', username):
        return ProbeResult('steam', CAT_PLATFORM, None, CONF_NONE,
                           'Handle del email no válido para Steam')
    url = f'https://steamcommunity.com/id/{username}'
    r = requests.get(url, headers={'User-Agent': UA}, timeout=10, allow_redirects=False)
    body = r.text or ''
    if r.status_code == 200 and 'The specified profile could not be found' not in body:
        return ProbeResult('steam', CAT_PLATFORM, True, CONF_MEDIUM,
                           f'Existe perfil público de Steam con el handle de tu email')
    return ProbeResult('steam', CAT_PLATFORM, False, CONF_LOW,
                       'No detectamos perfil de Steam con ese handle')


# --- Probes: Apps de citas (best-effort, frágiles) ---

@_service('bumble', CAT_DATING)
def probe_bumble(email: str | None, phone: str | None = None) -> ProbeResult:
    if not email:
        return ProbeResult('bumble', CAT_DATING, None, CONF_NONE, 'Sin email')
    # Bumble usa Cloudflare en su web — endpoint server-side a menudo bloquea.
    url = 'https://bumble.com/api/cmd?action=USER_EXISTS'
    try:
        r = requests.post(url, json={'email': email},
                          headers={'User-Agent': UA, 'Content-Type': 'application/json'},
                          timeout=10)
    except requests.RequestException as e:
        return ProbeResult('bumble', CAT_DATING, None, CONF_NONE,
                           f'Error de red contra Bumble: {e}')
    if _is_cloudflare_block(r.text, r.status_code):
        return ProbeResult('bumble', CAT_DATING, None, CONF_NONE,
                           'Cloudflare bloqueó la consulta server-side. Usa cookie del navegador.')
    return ProbeResult('bumble', CAT_DATING, None, CONF_LOW,
                       f'Bumble HTTP {r.status_code} — endpoint cambiado o requiere flujo extra')


@_service('badoo', CAT_DATING)
def probe_badoo(email: str | None, phone: str | None = None) -> ProbeResult:
    if not email:
        return ProbeResult('badoo', CAT_DATING, None, CONF_NONE, 'Sin email')
    # Badoo expone /signin endpoint que distingue email no encontrado.
    url = 'https://badoo.com/signin'
    try:
        r = requests.get(url, headers={'User-Agent': UA}, timeout=10)
    except requests.RequestException as e:
        return ProbeResult('badoo', CAT_DATING, None, CONF_NONE,
                           f'Error de red contra Badoo: {e}')
    if _is_cloudflare_block(r.text, r.status_code):
        return ProbeResult('badoo', CAT_DATING, None, CONF_NONE,
                           'Cloudflare bloqueó la consulta server-side')
    # Sin engine JS/cookie, Badoo no enumera de forma fiable. Marcamos no determinable.
    return ProbeResult('badoo', CAT_DATING, None, CONF_LOW,
                       'Badoo requiere flujo con cookie/CSRF para enumerar — no determinable server-side')


@_service('tinder', CAT_DATING)
def probe_tinder(email: str | None, phone: str | None = None) -> ProbeResult:
    """Tinder por email es prácticamente imposible (responde genérico).
    Por phone podemos tirar al endpoint de SMS code request, pero está
    fuertemente protegido y enviar SMS no consentido sería invasivo. Lo
    dejamos como 'no determinable' con honestidad.
    """
    return ProbeResult('tinder', CAT_DATING, None, CONF_NONE,
                       'Tinder no expone enumeración fiable server-side. '
                       'Para detectar requiere búsqueda por foto (FaceCheck).')


@_service('hinge', CAT_DATING)
def probe_hinge(email: str | None, phone: str | None = None) -> ProbeResult:
    return ProbeResult('hinge', CAT_DATING, None, CONF_NONE,
                       'Hinge requiere flujo con CSRF/cookie — no determinable server-side todavía.')


@_service('okcupid', CAT_DATING)
def probe_okcupid(email: str | None, phone: str | None = None) -> ProbeResult:
    if not email:
        return ProbeResult('okcupid', CAT_DATING, None, CONF_NONE, 'Sin email')
    url = 'https://www.okcupid.com/login'
    try:
        r = requests.get(url, headers={'User-Agent': UA}, timeout=10)
    except requests.RequestException as e:
        return ProbeResult('okcupid', CAT_DATING, None, CONF_NONE,
                           f'Error de red contra OkCupid: {e}')
    if _is_cloudflare_block(r.text, r.status_code):
        return ProbeResult('okcupid', CAT_DATING, None, CONF_NONE,
                           'Cloudflare bloqueó la consulta')
    return ProbeResult('okcupid', CAT_DATING, None, CONF_LOW,
                       'OkCupid requiere flujo con CSRF para enumerar — no determinable server-side')


@_service('pof', CAT_DATING)
def probe_pof(email: str | None, phone: str | None = None) -> ProbeResult:
    return ProbeResult('pof', CAT_DATING, None, CONF_NONE,
                       'POF requiere flujo con captcha — no determinable server-side todavía.')


@_service('meetic', CAT_DATING)
def probe_meetic(email: str | None, phone: str | None = None) -> ProbeResult:
    return ProbeResult('meetic', CAT_DATING, None, CONF_NONE,
                       'Meetic requiere flujo con CSRF — no determinable server-side todavía.')


@_service('happn', CAT_DATING)
def probe_happn(email: str | None, phone: str | None = None) -> ProbeResult:
    return ProbeResult('happn', CAT_DATING, None, CONF_NONE,
                       'Happn requiere flujo con OAuth — no determinable server-side todavía.')


# --- Probes: Foto (FaceCheck.id) ---

def probe_facecheck(image_data_b64: str | None) -> ProbeResult:
    """Sube una foto a FaceCheck.id y devuelve los matches encontrados.

    Requiere FACECHECK_API_KEY. Free tier: ~3 búsquedas/día. Plan pago: ~0,05€/búsqueda.
    El usuario paga 1 token EXTRA (=2 totales) cuando incluye foto.

    `image_data_b64`: bytes de la imagen en base64 (sin prefijo data:).
    """
    if not image_data_b64:
        return ProbeResult('facecheck', CAT_FACE, None, CONF_NONE, 'Sin foto')
    api_key = os.environ.get('FACECHECK_API_KEY', '').strip()
    if not api_key:
        return ProbeResult('facecheck', CAT_FACE, None, CONF_NONE,
                           'FACECHECK_API_KEY no configurada en el servidor')
    try:
        image_bytes = base64.b64decode(image_data_b64, validate=True)
    except Exception:
        return ProbeResult('facecheck', CAT_FACE, None, CONF_NONE,
                           'La imagen no es base64 válido')
    if len(image_bytes) > 5 * 1024 * 1024:
        return ProbeResult('facecheck', CAT_FACE, None, CONF_NONE,
                           'La imagen supera 5 MB')

    upload_url = 'https://facecheck.id/api/upload_pic'
    files = {'images': ('photo.jpg', image_bytes, 'image/jpeg')}
    headers = {'Authorization': api_key, 'Accept': 'application/json'}
    r = requests.post(upload_url, files=files, headers=headers, timeout=30)
    if r.status_code != 200:
        return ProbeResult('facecheck', CAT_FACE, None, CONF_NONE,
                           f'FaceCheck upload HTTP {r.status_code}: {(r.text or "")[:200]}')
    try:
        data = r.json()
    except Exception:
        return ProbeResult('facecheck', CAT_FACE, None, CONF_NONE,
                           'FaceCheck devolvió no-JSON en upload')
    id_search = data.get('id_search')
    if not id_search:
        return ProbeResult('facecheck', CAT_FACE, None, CONF_NONE,
                           f'FaceCheck no devolvió id_search: {data.get("message") or data}')

    # Polling al endpoint de búsqueda. FaceCheck tarda 5-30s.
    search_url = 'https://facecheck.id/api/search'
    payload = {'id_search': id_search, 'with_progress': True, 'status_only': False, 'demo': False}
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        time.sleep(3)
        rs = requests.post(search_url, json=payload, headers=headers, timeout=30)
        if rs.status_code != 200:
            return ProbeResult('facecheck', CAT_FACE, None, CONF_NONE,
                               f'FaceCheck search HTTP {rs.status_code}')
        try:
            sdata = rs.json()
        except Exception:
            continue
        if (sdata.get('error') or '').strip():
            return ProbeResult('facecheck', CAT_FACE, None, CONF_NONE,
                               f'FaceCheck error: {sdata["error"]}')
        if sdata.get('output') and isinstance(sdata['output'], dict):
            items = sdata['output'].get('items') or []
            matches = [{
                'url': it.get('url'),
                'score': it.get('score'),
                'thumbnail': it.get('base64'),
            } for it in items[:20]]
            n = len(matches)
            if n == 0:
                return ProbeResult('facecheck', CAT_FACE, False, CONF_HIGH,
                                   'FaceCheck no encontró tu cara en su índice público')
            return ProbeResult('facecheck', CAT_FACE, True, CONF_HIGH,
                               f'FaceCheck encontró {n} sitio(s) con tu cara',
                               extra={'matches': matches})
    return ProbeResult('facecheck', CAT_FACE, None, CONF_NONE,
                       'FaceCheck tardó demasiado en responder.')


# --- Orquestador ---

EMAIL_PROBES: list = [
    probe_hibp,
    probe_github, probe_spotify, probe_strava, probe_reddit, probe_steam,
    probe_bumble, probe_badoo, probe_okcupid, probe_pof, probe_meetic, probe_happn,
    probe_tinder, probe_hinge,
]


def run_full_scan(
    email: str | None,
    phone: str | None,
    image_data_b64: str | None = None,
    log_fn: Callable[[str], None] | None = None,
    max_workers: int = 6,
) -> dict:
    """Ejecuta todos los probes en paralelo y devuelve el dict serializable.

    log_fn: si se pasa, recibe líneas de log para streamear al frontend.
    """
    log_fn = log_fn or (lambda m: None)
    email = _normalize_email(email or '')
    phone = _normalize_phone(phone or '')

    if not email and not phone and not image_data_b64:
        raise ValueError('Necesito al menos un dato (email, phone o foto).')

    results: list[ProbeResult] = []
    log_fn(f'🔎 Iniciando scan — email={"sí" if email else "no"} phone={"sí" if phone else "no"} foto={"sí" if image_data_b64 else "no"}')

    if email or phone:
        log_fn(f'📡 Lanzando {len(EMAIL_PROBES)} probes de huella…')
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(_measure, p, email, phone): p for p in EMAIL_PROBES}
            for fut in as_completed(futures):
                r = fut.result()
                results.append(r)
                emoji = '✓' if r.found else ('✗' if r.found is False else '❓')
                log_fn(f'   {emoji} {r.service}: {r.detail}  [{r.confidence}, {r.duration_ms}ms]')

    if image_data_b64:
        log_fn('📸 Búsqueda por foto en FaceCheck.id (puede tardar 30-60s)…')
        face_result = _measure(probe_facecheck, image_data_b64)
        results.append(face_result)
        emoji = '✓' if face_result.found else ('✗' if face_result.found is False else '❓')
        log_fn(f'   {emoji} facecheck: {face_result.detail}  [{face_result.confidence}]')

    # Score: 100 = expuesto del todo, 0 = limpio
    score = _compute_exposure_score(results)
    log_fn(f'🧮 Score de exposición: {score}/100')

    return {
        'email_masked': _mask_email(email) if email else None,
        'phone_masked': _mask_phone(phone) if phone else None,
        'email_hash': _hash_input(email) if email else None,
        'phone_hash': _hash_input(phone) if phone else None,
        'face_used': bool(image_data_b64),
        'score': score,
        'probes': [r.to_dict() for r in sorted(results, key=lambda x: (x.category, x.service))],
        'recommendations': _build_recommendations(results),
        'summary_by_category': _summary_by_category(results),
    }


def _compute_exposure_score(results: list[ProbeResult]) -> int:
    """Suma ponderada por categoría. Heurística simple."""
    weights = {CAT_BREACH: 8, CAT_DATING: 6, CAT_PLATFORM: 2, CAT_FACE: 4}
    total_w = 0.0
    score = 0.0
    for r in results:
        if r.found is True:
            w = weights.get(r.category, 1)
            score += w * (1.0 if r.confidence == CONF_HIGH else 0.6 if r.confidence == CONF_MEDIUM else 0.3)
            total_w += w
        elif r.found is False:
            total_w += weights.get(r.category, 1) * 0.4  # cuenta poco
    if total_w <= 0:
        return 0
    return int(min(100, round((score / total_w) * 100)))


def _summary_by_category(results: list[ProbeResult]) -> dict:
    summary = {}
    for r in results:
        bucket = summary.setdefault(r.category, {'found': 0, 'not_found': 0, 'unknown': 0, 'total': 0})
        bucket['total'] += 1
        if r.found is True:
            bucket['found'] += 1
        elif r.found is False:
            bucket['not_found'] += 1
        else:
            bucket['unknown'] += 1
    return summary


def _build_recommendations(results: list[ProbeResult]) -> list[dict]:
    recs: list[dict] = []
    by_service = {r.service: r for r in results}
    hibp = by_service.get('hibp')
    if hibp and hibp.found:
        breaches = (hibp.extra or {}).get('breaches') or []
        recs.append({
            'level': 'high',
            'message': f'Apareces en {len(breaches)} filtraciones de datos. '
                       'Cambia tu password si la reusas en otros sitios y activa 2FA.',
            'action': 'reset_passwords',
        })
    dating_found = [r for r in results if r.category == CAT_DATING and r.found]
    if dating_found:
        recs.append({
            'level': 'medium',
            'message': f'Tu email/teléfono aparece en {len(dating_found)} apps de citas. '
                       'Si ya no las usas, elimina las cuentas.',
            'action': 'delete_accounts',
            'services': [r.service for r in dating_found],
        })
    face = by_service.get('facecheck')
    if face and face.found:
        n = len((face.extra or {}).get('matches') or [])
        recs.append({
            'level': 'medium',
            'message': f'Tu cara aparece en {n} sitio(s) públicos indexados.',
            'action': 'review_public_photos',
        })
    if not recs:
        recs.append({'level': 'info', 'message': 'Sin acciones críticas pendientes.', 'action': 'none'})
    return recs
