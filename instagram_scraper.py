"""
HokoSocial — Instagram scraper (server-side).

Trabaja en dos modos:
- Anónimo: solo perfil público + 12 posts. Falla con 401/403/429 si la IP
  está marcada (típico desde Render).
- Autenticado: con un dict `cookies` válido (de una cuenta-bot logueada),
  desbloquea stories, highlights, followers, following, tagged, y libera el
  bloqueo de IP de Render porque IG nos atiende como user logueado.

Cuando IG rechaza la cookie (login_required / challenge_required) lanza
InstagramAuthExpiredError para que el caller marque la cuenta burned.
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
    """IG bloqueó por IP / rate limit (401/403/429 sin cookie válida)."""


class InstagramAuthExpiredError(Exception):
    """La cookie de la cuenta-bot ya no es válida (login_required, challenge_required, checkpoint)."""


class InstagramNotFoundError(Exception):
    """username inexistente."""


class InstagramScrapeError(Exception):
    """Otros fallos."""


def _headers(cookies: dict | None = None, extra: dict | None = None) -> dict:
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
    if cookies and cookies.get('csrftoken'):
        h['X-CSRFToken'] = cookies['csrftoken']
    if extra:
        h.update(extra)
    return h


def _cookie_jar(cookies: dict | None) -> dict | None:
    if not cookies:
        return None
    # requests acepta un dict {name: value}
    return {k: v for k, v in cookies.items() if v}


def _detect_auth_problem(payload: dict | None) -> bool:
    """Detecta respuesta de 'cookie quemada'."""
    if not isinstance(payload, dict):
        return False
    msg = (payload.get('message') or '').lower()
    return (
        payload.get('require_login')
        or payload.get('login_required')
        or 'login_required' in msg
        or 'checkpoint' in msg
        or 'challenge_required' in msg
        or 'spam' in msg
    )


def _get(url: str, cookies: dict | None = None, retries: int = 1, timeout: int = 12) -> requests.Response:
    last_status = None
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=_headers(cookies), cookies=_cookie_jar(cookies), timeout=timeout)
        except Exception as e:
            if attempt < retries:
                time.sleep(1.0)
                continue
            raise InstagramScrapeError(f'Network error: {e}')

        last_status = r.status_code
        if r.status_code == 200:
            return r
        if r.status_code == 404:
            raise InstagramNotFoundError(f'404 not found at {url}')
        if r.status_code in (401, 403, 429):
            # Si tenemos cookie y nos rechaza, posiblemente está quemada.
            if cookies:
                try:
                    body = r.json()
                except Exception:
                    body = None
                if _detect_auth_problem(body):
                    raise InstagramAuthExpiredError(
                        f'Cookie inválida o quemada (HTTP {r.status_code}: {body or r.text[:120]})'
                    )
            raise InstagramBlockedError(f'HTTP {r.status_code} (login or rate-limit)')
        if attempt < retries:
            time.sleep(1.0)
    raise InstagramScrapeError(f'HTTP {last_status} after retries')


# ---------- Profile (web_profile_info) ----------

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


def _feed_item(item: dict) -> dict:
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
        'product_type': item.get('product_type'),
        'caption': ((item.get('caption') or {}).get('text') or '')[:280],
        'likes': item.get('like_count', 0),
        'comments': item.get('comment_count', 0),
        'taken_at': item.get('taken_at'),
        'view_count': item.get('play_count') or item.get('view_count') or 0,
    }


def _user_summary(u: dict) -> dict:
    return {
        'username': u.get('username'),
        'full_name': u.get('full_name'),
        'profile_pic_url': u.get('profile_pic_url'),
        'is_verified': bool(u.get('is_verified')),
        'is_private': bool(u.get('is_private')),
        'pk': u.get('pk') or u.get('id'),
    }


def parse_user(data: dict) -> dict:
    user = (data.get('data') or {}).get('user')
    if not user:
        raise InstagramScrapeError('Estructura inesperada en web_profile_info.')

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


def scrape_profile(username: str, cookies: dict | None = None) -> dict:
    username = (username or '').strip().lstrip('@').lower()
    if not username:
        raise InstagramScrapeError('Username vacío.')
    url = f'https://www.instagram.com/api/v1/users/web_profile_info/?username={username}'
    r = _get(url, cookies=cookies)
    try:
        return parse_user(r.json())
    except ValueError:
        raise InstagramScrapeError('Respuesta no-JSON.')


# ---------- Feed (timeline) ----------

def scrape_feed(user_id: str, max_id: str | None = None, count: int = 12, cookies: dict | None = None) -> dict:
    base = f'https://i.instagram.com/api/v1/feed/user/{user_id}/?count={count}'
    if max_id:
        base += f'&max_id={max_id}'
    r = _get(base, cookies=cookies)
    data = r.json()
    items = data.get('items') or []
    return {
        'items': [_feed_item(it) for it in items if (it.get('product_type') or '') != 'story'],
        'next_max_id': data.get('next_max_id'),
        'more_available': bool(data.get('more_available')),
    }


# ---------- Reels (subset del feed) ----------

def scrape_reels(user_id: str, max_id: str | None = None, count: int = 12, cookies: dict | None = None) -> dict:
    base = f'https://i.instagram.com/api/v1/feed/user/{user_id}/?count={count * 2}'
    if max_id:
        base += f'&max_id={max_id}'
    r = _get(base, cookies=cookies)
    data = r.json()
    items = data.get('items') or []
    reels_only = [_feed_item(it) for it in items if (it.get('product_type') == 'clips')]
    return {
        'items': reels_only[:count],
        'next_max_id': data.get('next_max_id'),
        'more_available': bool(data.get('more_available')),
    }


# ---------- Stories (requiere cookie) ----------

def scrape_stories(user_id: str, cookies: dict) -> dict:
    if not cookies:
        raise InstagramScrapeError('Stories requiere cookies de cuenta-bot.')
    url = f'https://i.instagram.com/api/v1/feed/reels_media/?reel_ids={user_id}'
    r = _get(url, cookies=cookies)
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


# ---------- Highlights (requiere cookie) ----------

def scrape_highlights(user_id: str, cookies: dict) -> dict:
    if not cookies:
        raise InstagramScrapeError('Highlights requiere cookies de cuenta-bot.')
    url = f'https://i.instagram.com/api/v1/highlights/{user_id}/highlights_tray/'
    r = _get(url, cookies=cookies)
    data = r.json()
    tray = data.get('tray') or []
    items = []
    for h in tray:
        cover = h.get('cover_media') or {}
        thumb_versions = (cover.get('cropped_image_version') or {})
        items.append({
            'id': h.get('id'),
            'title': h.get('title'),
            'cover_url': thumb_versions.get('url') or (h.get('cover_media') or {}).get('url'),
            'media_count': h.get('media_count'),
            'created_at': h.get('created_at'),
        })
    return {'items': items, 'count': len(items)}


# ---------- Followers / Following (requieren cookie) ----------

def _friendship_list(user_id: str, cookies: dict, kind: str, max_id: str | None = None, count: int = 24) -> dict:
    if not cookies:
        raise InstagramScrapeError(f'{kind.capitalize()} requiere cookies de cuenta-bot.')
    base = f'https://i.instagram.com/api/v1/friendships/{user_id}/{kind}/?count={count}'
    if max_id:
        base += f'&max_id={max_id}'
    r = _get(base, cookies=cookies)
    data = r.json()
    users_raw = data.get('users') or []
    return {
        'items': [_user_summary(u) for u in users_raw],
        'next_max_id': data.get('next_max_id'),
        'has_more': bool(data.get('big_list')) or bool(data.get('next_max_id')),
        'total_count': data.get('total_count'),
    }


def scrape_followers(user_id: str, cookies: dict, max_id: str | None = None, count: int = 24) -> dict:
    return _friendship_list(user_id, cookies, 'followers', max_id, count)


def scrape_following(user_id: str, cookies: dict, max_id: str | None = None, count: int = 24) -> dict:
    return _friendship_list(user_id, cookies, 'following', max_id, count)


# ---------- User info detallado (con cookie da has_active_story etc) ----------

def scrape_user_info(user_id: str, cookies: dict | None = None) -> dict:
    """Devuelve flags útiles: has_active_story, story_count, etc."""
    url = f'https://i.instagram.com/api/v1/users/{user_id}/info/'
    r = _get(url, cookies=cookies)
    data = r.json()
    user = data.get('user') or {}
    return {
        'has_active_story': bool(user.get('has_active_story') or user.get('story_count')),
        'story_count': user.get('story_count', 0),
        'highlight_reel_count': user.get('highlight_reel_count', 0),
    }
