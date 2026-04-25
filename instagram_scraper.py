"""
HokoSocial — Instagram public-profile scraper (server-side).

Hace una sola request HTTP al endpoint web_profile_info de Instagram (el
que usa instagram.com en el navegador). NO necesita login.

Funciona desde residential IPs siempre. Desde IPs de cloud (Render, AWS,
GCP) Instagram puede bloquear con 401/403/429 si el ASN está marcado.
En ese caso, el caller debería tener un plan B (worker en PC del user,
proxy residencial, etc.).

Es exactamente la misma lógica que en worker_instagram.py — duplicada
intencionalmente para que cada repo sea autosuficiente.
"""

from __future__ import annotations

import time
from typing import Callable

import requests

LogFn = Callable[[str], None]

UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'
)
APP_ID = '936619743392459'  # IG Web App ID público


class InstagramBlockedError(Exception):
    """Instagram nos está bloqueando (401/403/429 con cuerpo de bloqueo)."""


class InstagramNotFoundError(Exception):
    """El username no existe en Instagram."""


class InstagramScrapeError(Exception):
    """Otros fallos (red, JSON inválido, estructura inesperada)."""


def _headers() -> dict:
    return {
        'User-Agent': UA,
        'X-IG-App-ID': APP_ID,
        'Accept': '*/*',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
        'Referer': 'https://www.instagram.com/',
        'Origin': 'https://www.instagram.com',
        'sec-ch-ua': '"Chromium";v="127", "Not)A;Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }


def _fetch(username: str, retries: int = 1, log: LogFn | None = None) -> dict:
    url = f'https://www.instagram.com/api/v1/users/web_profile_info/?username={username}'
    last_error = None
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=_headers(), timeout=12)
        except Exception as e:
            last_error = e
            if attempt < retries:
                if log: log(f'Network error: {e}, reintentando...')
                time.sleep(1.5)
                continue
            raise InstagramScrapeError(f'No pudimos contactar Instagram: {e}')

        if r.status_code == 200:
            try:
                return r.json()
            except ValueError:
                raise InstagramScrapeError('Instagram devolvió un cuerpo no-JSON.')
        if r.status_code == 404:
            raise InstagramNotFoundError(f'@{username} no existe.')
        if r.status_code in (401, 403, 429):
            raise InstagramBlockedError(
                f'Instagram bloqueó la petición (HTTP {r.status_code}). '
                'IP probablemente marcada como datacenter.'
            )
        last_error = f'HTTP {r.status_code}'
        if attempt < retries:
            time.sleep(1.5)
    raise InstagramScrapeError(f'Fallo tras reintentos: {last_error}')


def _post_summary(node: dict) -> dict:
    return {
        'shortcode': node.get('shortcode'),
        'thumbnail': node.get('thumbnail_src') or node.get('display_url'),
        'is_video': bool(node.get('is_video')),
        'caption': (
            node.get('edge_media_to_caption', {})
                .get('edges', [{}])[0]
                .get('node', {})
                .get('text', '')
        )[:280],
        'likes': (node.get('edge_liked_by') or node.get('edge_media_preview_like') or {}).get('count', 0),
        'comments': (node.get('edge_media_to_comment') or {}).get('count', 0),
        'taken_at': node.get('taken_at_timestamp'),
        'view_count': node.get('video_view_count') or 0,
    }


def parse_user(data: dict) -> dict:
    user = (data.get('data') or {}).get('user')
    if not user:
        raise InstagramScrapeError('Estructura de respuesta inesperada.')

    posts_edges = ((user.get('edge_owner_to_timeline_media') or {}).get('edges') or [])
    recent_posts = [_post_summary(e.get('node') or {}) for e in posts_edges[:12]]

    return {
        'username': user.get('username'),
        'full_name': user.get('full_name'),
        'biography': user.get('biography'),
        'profile_pic_url': user.get('profile_pic_url_hd') or user.get('profile_pic_url'),
        'external_url': user.get('external_url'),
        'is_private': bool(user.get('is_private')),
        'is_verified': bool(user.get('is_verified')),
        'is_business_account': bool(user.get('is_business_account')),
        'category_name': user.get('category_name'),
        'followers': (user.get('edge_followed_by') or {}).get('count', 0),
        'following': (user.get('edge_follow') or {}).get('count', 0),
        'posts_count': (user.get('edge_owner_to_timeline_media') or {}).get('count', 0),
        'recent_posts': recent_posts,
        'profile_url': f'https://www.instagram.com/{user.get("username")}/',
    }


def scrape_profile(username: str) -> dict:
    """Scrapea el perfil público. Lanza Instagram*Error según el caso."""
    username = (username or '').strip().lstrip('@').lower()
    if not username:
        raise InstagramScrapeError('Username vacío.')
    data = _fetch(username)
    return parse_user(data)
