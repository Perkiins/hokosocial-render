"""
HokoSocial — Instagram public-profile scraper (server-side).

Funciona SIN login. Cada función puede fallar con InstagramBlockedError
si la IP está marcada (típico desde datacenter); el caller decidirá si
fallback al worker.

Endpoints públicos verificados (residencial):
- web_profile_info        → perfil base + 12 posts.
- users/{id}/info         → info detallada.
- feed/user/{id}          → timeline paginado (todos los posts).
- feed/reels_media        → stories activas.
- /{username}/ (HTML)     → highlights tray embebido.
- /{username}/tagged/     → tagged HTML.

Endpoints que NO funcionan sin login (verificado):
- friendships/{id}/followers → 401, require_login.
- friendships/{id}/following → 401, require_login.
"""

from __future__ import annotations

import json
import re
import time
from typing import Callable

import requests

LogFn = Callable[[str], None]

UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'
)
APP_ID = '936619743392459'


class InstagramBlockedError(Exception):
    pass


class InstagramNotFoundError(Exception):
    pass


class InstagramScrapeError(Exception):
    pass


def _headers(extra: dict | None = None) -> dict:
    h = {
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
    if extra:
        h.update(extra)
    return h


def _get(url: str, retries: int = 1, timeout: int = 12, accept_html: bool = False) -> requests.Response:
    last_status = None
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=_headers(), timeout=timeout)
        except Exception as e:
            if attempt < retries:
                time.sleep(1.0)
                continue
            raise InstagramScrapeError(f'Network error: {e}')

        last_status = r.status_code
        if r.status_code == 200 or (accept_html and r.status_code in (200, 201)):
            return r
        if r.status_code == 404:
            raise InstagramNotFoundError(f'404 not found at {url}')
        if r.status_code in (401, 403, 429):
            raise InstagramBlockedError(f'HTTP {r.status_code} (login or rate-limit)')
        if attempt < retries:
            time.sleep(1.0)
    raise InstagramScrapeError(f'HTTP {last_status} after retries')


# ---------- Profile (base) ----------

def _post_summary(node: dict) -> dict:
    """De edge_owner_to_timeline_media (web_profile_info)."""
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


def _feed_item(item: dict) -> dict:
    """De feed/user/{id} (estructura distinta)."""
    image_versions = (item.get('image_versions2') or {}).get('candidates') or []
    thumb = image_versions[0]['url'] if image_versions else None
    if not thumb and item.get('carousel_media'):
        first = item['carousel_media'][0]
        ivs = (first.get('image_versions2') or {}).get('candidates') or []
        if ivs:
            thumb = ivs[0]['url']
    media_type = item.get('media_type')
    return {
        'shortcode': item.get('code'),
        'thumbnail': thumb,
        'is_video': media_type == 2,
        'is_carousel': media_type == 8,
        'product_type': item.get('product_type'),  # 'clips' = reel
        'caption': ((item.get('caption') or {}).get('text') or '')[:280],
        'likes': item.get('like_count', 0),
        'comments': item.get('comment_count', 0),
        'taken_at': item.get('taken_at'),
        'view_count': item.get('play_count') or item.get('view_count') or 0,
    }


def parse_user(data: dict) -> dict:
    user = (data.get('data') or {}).get('user')
    if not user:
        raise InstagramScrapeError('Estructura de respuesta inesperada.')

    posts_edges = ((user.get('edge_owner_to_timeline_media') or {}).get('edges') or [])
    recent_posts = [_post_summary(e.get('node') or {}) for e in posts_edges[:12]]

    return {
        'id': user.get('id'),
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
        'has_clips': bool(user.get('has_clips')),
        'recent_posts': recent_posts,
        'profile_url': f'https://www.instagram.com/{user.get("username")}/',
    }


def scrape_profile(username: str) -> dict:
    username = (username or '').strip().lstrip('@').lower()
    if not username:
        raise InstagramScrapeError('Username vacío.')
    url = f'https://www.instagram.com/api/v1/users/web_profile_info/?username={username}'
    r = _get(url)
    try:
        return parse_user(r.json())
    except ValueError:
        raise InstagramScrapeError('Respuesta no-JSON.')


# ---------- Feed completo (timeline) paginado ----------

def scrape_feed(user_id: str, max_id: str | None = None, count: int = 12) -> dict:
    """Devuelve {items: [...], next_max_id: str|None, more_available: bool}."""
    base = f'https://i.instagram.com/api/v1/feed/user/{user_id}/?count={count}'
    if max_id:
        base += f'&max_id={max_id}'
    r = _get(base)
    data = r.json()
    items = data.get('items') or []
    return {
        'items': [_feed_item(it) for it in items if (it.get('product_type') or '') != 'story'],
        'next_max_id': data.get('next_max_id'),
        'more_available': bool(data.get('more_available')),
    }


# ---------- Stories ----------

def scrape_stories(user_id: str) -> dict:
    """Stories activas. Si no tiene, devuelve items vacío."""
    url = f'https://i.instagram.com/api/v1/feed/reels_media/?reel_ids={user_id}'
    r = _get(url)
    data = r.json()
    reels = data.get('reels') or {}
    reel = reels.get(str(user_id)) or {}
    items_raw = reel.get('items') or []
    items = []
    for it in items_raw:
        ivs = (it.get('image_versions2') or {}).get('candidates') or []
        thumb = ivs[0]['url'] if ivs else None
        vids = it.get('video_versions') or []
        video_url = vids[0]['url'] if vids else None
        items.append({
            'id': it.get('pk'),
            'thumbnail': thumb,
            'video_url': video_url,
            'is_video': bool(video_url),
            'taken_at': it.get('taken_at'),
            'expiring_at': it.get('expiring_at'),
        })
    return {
        'items': items,
        'count': len(items),
        'has_active': len(items) > 0,
    }


# ---------- Highlights (parseando la página HTML del perfil) ----------

_HIGHLIGHTS_RE = re.compile(
    r'"highlight_reels":\s*(\[.*?\])',
    re.DOTALL,
)


def scrape_highlights(username: str) -> dict:
    """Highlights tray: solo metadatos (id, title, cover_url). Cubre la página
    pública que ya hace SSR con datos embebidos. Si no tiene highlights, list
    vacío.

    El endpoint JSON propio de highlights requiere login; este truco usa el
    HTML público que sí los lista.
    """
    url = f'https://www.instagram.com/{username}/'
    r = _get(url, accept_html=True)
    html = r.text
    items = []
    # Forma 1: GraphQL embebido (Instagram pre-rendera SOMETIMES)
    m = _HIGHLIGHTS_RE.search(html)
    if m:
        try:
            arr = json.loads(m.group(1))
            for h in arr:
                items.append({
                    'id': h.get('id'),
                    'title': h.get('title'),
                    'cover_url': (h.get('cover_media') or {}).get('thumbnail_src') or h.get('cover_media_cropped_thumbnail') or None,
                })
        except Exception:
            pass

    # Forma 2: el endpoint de la versión web también expone tray a través de
    # un GraphQL POST que requiere doc_id. Como muchas veces el HTML no embebe,
    # devolvemos lo que tengamos. Si está vacío, el frontend muestra "no hay".
    return {
        'items': items,
        'count': len(items),
        'note': 'Si no se ven highlights, Instagram puede haber cambiado el SSR. '
                'En ese caso solo se obtendrían con login.',
    }


# ---------- Tagged (etiquetadas) ----------

def scrape_tagged(username: str, count: int = 12) -> dict:
    """Tagged feed: parsea la página HTML pública /{username}/tagged/."""
    url = f'https://www.instagram.com/{username}/tagged/'
    r = _get(url, accept_html=True)
    # El feed embebido va en application/json scripts. Implementación mínima:
    # solo confirmamos disponibilidad. El parsing fino requiere doc_id y queda
    # para iteración futura.
    has = '"tagged"' in r.text or 'usertags' in r.text
    return {
        'items': [],
        'available': has,
        'note': 'Etiquetadas requieren un GraphQL adicional con doc_id. '
                'En esta versión solo confirmamos que la página existe.',
    }


# ---------- Reels (subset del feed) ----------

def scrape_reels(user_id: str, max_id: str | None = None, count: int = 12) -> dict:
    """No hay endpoint público sólo para reels que funcione sin login.
    Como workaround, recorremos el feed y filtramos product_type='clips'."""
    base = f'https://i.instagram.com/api/v1/feed/user/{user_id}/?count={count * 2}'
    if max_id:
        base += f'&max_id={max_id}'
    r = _get(base)
    data = r.json()
    items = data.get('items') or []
    reels_only = [_feed_item(it) for it in items if (it.get('product_type') == 'clips')]
    return {
        'items': reels_only[:count],
        'next_max_id': data.get('next_max_id'),
        'more_available': bool(data.get('more_available')),
    }
