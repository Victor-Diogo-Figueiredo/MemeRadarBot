#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CAÇADOR DE MOEDAS NOVAS (Pump.fun)

Bot INDEPENDENTE: este arquivo roda sozinho, sem precisar do bot_melhorado.py.
Basta uma pasta com bot_cacador.py e um .env (os arquivos de estado são criados ao lado do script).
Pode rodar ao mesmo tempo que o radar, porque usa arquivos de estado próprios.

O que faz:
1. Escuta cada token recém-lançado na Pump.fun (PumpPortal) em tempo real.
2. Lê os metadados do lançamento (X, site, Telegram, descrição) e classifica a narrativa:
   meme viral, jogo, celebridade/famoso, tema em alta, link de post ou comunidade no X.
3. Mede a tração inicial no DexScreener (compras, vendas, volume, market cap).
4. Calcula um score de POTENCIAL (0-100) e avisa no Telegram, com poucas travas, explicando os riscos.
5. Acompanha cada alerta (2x, 3x, 5x, 10x, recuo do topo) e mostra o desempenho real do bot.

Ele NÃO compra nem vende nada. As decisões e operações são sempre suas.
Score alto NÃO é garantia: a maioria dos tokens novos da Pump.fun perde quase todo o valor.
"""

import os
import re
import sys
import json
import time
import html
import asyncio
import logging
import statistics
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import aiohttp
import websockets
from dotenv import load_dotenv
from telegram import Bot, Update
from telegram.error import BadRequest, Forbidden, RetryAfter
from telegram.ext import Application, CommandHandler, ContextTypes

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")   # .env ao lado do script
load_dotenv()                    # e também um .env na pasta atual, se houver
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
for _noisy in ("httpx", "httpcore"):   # estes logs imprimem a URL do Telegram com o token do bot
    logging.getLogger(_noisy).setLevel(logging.WARNING)
log = logging.getLogger("cacador")


# ==========================================
# Infraestrutura própria (HTTP, RPC Solana, segurança on-chain, busca do CA no X)
# ==========================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "").strip()
if HELIUS_API_KEY:
    SOLANA_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
else:
    SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com").strip()

PUMPPORTAL_WS_URL = "wss://pumpportal.fun/api/data"
DEX_BASE = "https://api.dexscreener.com"
RUGCHECK_BASE = "https://api.rugcheck.xyz/v1"
PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
RAYDIUM_AUTHORITY = "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1"
BURN_ADDRESSES = {
    "11111111111111111111111111111111",
    "1nc1nerator11111111111111111111111111111111",
    "dead111111111111111111111111111111111111111",
}
ENABLE_BUNDLED_DETECTION = os.getenv("ENABLE_BUNDLED_DETECTION", "true").lower() == "true"
ENABLE_X_SEARCH = os.getenv("ENABLE_X_SEARCH", "false").lower() == "true"
X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN", "").strip()
X_CACHE_SECONDS = max(60, int(os.getenv("X_CACHE_SECONDS", "900") or 900))

session: Optional[aiohttp.ClientSession] = None


async def get_json(url: str, headers: dict | None = None, params: dict | None = None, timeout_sec: int = 12):
    assert session is not None
    try:
        async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=timeout_sec)) as r:
            if r.status != 200:
                log.debug("HTTP %s em %s", r.status, url)
                return None
            return await r.json(content_type=None)
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
        log.debug("Falha consultando %s: %s", url, e)
        return None

async def call_solana_rpc(method: str, params: list, timeout_sec: int = 10) -> Any:
    assert session is not None
    payload = {"jsonrpc": "2.0", "id": int(time.time() * 1000) % 100000, "method": method, "params": params}
    try:
        async with session.post(SOLANA_RPC_URL, json=payload, timeout=aiohttp.ClientTimeout(total=timeout_sec)) as r:
            if r.status != 200:
                log.debug("RPC %s: HTTP %s", method, r.status)
                return None
            data = await r.json()
            if isinstance(data, dict) and data.get("error"):
                log.debug("RPC %s: %s", method, data["error"])
                return None
            return data.get("result")
    except Exception as e:
        log.debug("Erro na chamada RPC %s: %s", method, e)
        return None


@dataclass
class SecurityReport:
    mint: str
    mint_authority: Optional[str] = None
    freeze_authority: Optional[str] = None
    mint_revoked: bool = False
    freeze_revoked: bool = False
    max_single_holder_pct: float = 0.0
    top10_holders_pct: float = 0.0
    top_holder_address: str = ""
    pool_holder_pct: float = 0.0
    bundled_detected: bool = False
    bundled_wallets_count: int = 0
    bundled_total_pct: float = 0.0
    bundled_avg_pct: float = 0.0
    rugcheck_score: Optional[int] = None
    rugcheck_risks: list[str] = field(default_factory=list)
    raw_holders_count: int = 0


def detect_bundled_wallets(holders: list[dict], pool_addresses: set[str]) -> tuple[bool, int, float, float]:
    """Procura 3+ carteiras com saldos quase idênticos (sinal de lançamento com carteiras combinadas)."""
    user_holders = [
        h for h in holders
        if h.get("address") not in pool_addresses
        and h.get("owner") not in pool_addresses
        and h.get("pct", 0) >= 2.2
    ]
    if len(user_holders) < 3:
        return False, 0, 0.0, 0.0

    user_holders.sort(key=lambda x: x.get("pct", 0), reverse=True)
    visited = set()
    best_cluster: list[dict] = []
    for i in range(len(user_holders)):
        if i in visited:
            continue
        cluster = [user_holders[i]]
        base_pct = user_holders[i].get("pct", 0)
        for j in range(i + 1, len(user_holders)):
            other_pct = user_holders[j].get("pct", 0)
            diff = abs(base_pct - other_pct)
            rel_diff = diff / base_pct if base_pct > 0 else 1.0
            if diff <= 0.40 or rel_diff <= 0.08:
                cluster.append(user_holders[j])
                visited.add(j)
        if len(cluster) >= 3 and len(cluster) > len(best_cluster):
            if sum(c.get("pct", 0) for c in cluster) >= 8.0:
                best_cluster = cluster

    if len(best_cluster) >= 3:
        total = sum(c.get("pct", 0) for c in best_cluster)
        return True, len(best_cluster), total, total / len(best_cluster)
    return False, 0, 0.0, 0.0


async def check_token_security(mint: str) -> SecurityReport:
    """Autoridades do mint, concentração de holders e suspeita de multi-carteiras (RPC + Rugcheck)."""
    report = SecurityReport(mint=mint)
    pool_addresses = set(BURN_ADDRESSES)
    pool_addresses.add(PUMP_PROGRAM_ID)
    pool_addresses.add(RAYDIUM_AUTHORITY)

    mint_acc = await call_solana_rpc("getAccountInfo", [mint, {"encoding": "jsonParsed"}])
    if mint_acc and isinstance(mint_acc, dict):
        info = (((mint_acc.get("value") or {}).get("data") or {}).get("parsed") or {}).get("info") or {}
        report.mint_authority = info.get("mintAuthority")
        report.freeze_authority = info.get("freezeAuthority")
        report.mint_revoked = report.mint_authority is None
        report.freeze_revoked = report.freeze_authority is None

    rc_data = await get_json(f"{RUGCHECK_BASE}/tokens/{mint}/report")
    holders_list: list[dict] = []
    if rc_data and isinstance(rc_data, dict):
        report.rugcheck_score = rc_data.get("score")
        risks = rc_data.get("risks") or []
        report.rugcheck_risks = [str(r.get("name") or r.get("description") or "Risco") for r in risks[:6] if isinstance(r, dict)]

        if report.mint_authority is None and "mintAuthority" in rc_data:
            report.mint_authority = rc_data.get("mintAuthority")
            report.mint_revoked = report.mint_authority is None
        if report.freeze_authority is None and "freezeAuthority" in rc_data:
            report.freeze_authority = rc_data.get("freezeAuthority")
            report.freeze_revoked = report.freeze_authority is None

        for m in rc_data.get("markets") or []:
            if isinstance(m, dict):
                lp_acc = (m.get("lp") or {}).get("lpAccount") or m.get("pool")
                if lp_acc:
                    pool_addresses.add(lp_acc)

        for h in rc_data.get("topHolders") or []:
            if isinstance(h, dict):
                holders_list.append({
                    "address": h.get("address", ""), "owner": h.get("owner", ""),
                    "pct": float(h.get("pct", 0) or 0), "insider": bool(h.get("insider", False)),
                })

    if not holders_list:  # plano B pelo RPC
        largest = await call_solana_rpc("getTokenLargestAccounts", [mint])
        if largest and isinstance(largest, dict):
            supply_acc = await call_solana_rpc("getTokenSupply", [mint])
            total_supply = float(((supply_acc or {}).get("value") or {}).get("uiAmount") or 1.0)
            for acc in largest.get("value") or []:
                ui_amt = float(acc.get("uiAmount") or 0)
                holders_list.append({
                    "address": acc.get("address", ""), "owner": "",
                    "pct": (ui_amt / total_supply * 100) if total_supply > 0 else 0, "insider": False,
                })

    report.raw_holders_count = len(holders_list)

    user_holders = []
    for h in holders_list:
        addr, owner, pct = h.get("address", ""), h.get("owner", ""), h.get("pct", 0)
        if addr in pool_addresses or owner in pool_addresses:
            report.pool_holder_pct += pct
            continue
        if pct > 40.0 and len(user_holders) == 0:  # provavelmente a bonding curve/pool
            report.pool_holder_pct += pct
            continue
        user_holders.append(h)

    if user_holders:
        user_holders.sort(key=lambda x: x.get("pct", 0), reverse=True)
        report.max_single_holder_pct = user_holders[0].get("pct", 0)
        report.top_holder_address = user_holders[0].get("address", "")
        report.top10_holders_pct = sum(h.get("pct", 0) for h in user_holders[:10])

    if ENABLE_BUNDLED_DETECTION and user_holders:
        is_bundled, count, total_pct, avg_pct = detect_bundled_wallets(user_holders, pool_addresses)
        if is_bundled:
            report.bundled_detected = True
            report.bundled_wallets_count = count
            report.bundled_total_pct = total_pct
            report.bundled_avg_pct = avg_pct

    return report


_x_ca_cache: dict[str, tuple[float, int]] = {}

async def x_ca_mentions(mint: str) -> Optional[int]:
    """Posts recentes (7 dias, sem retweets) que contêm o endereço do contrato. Cada post lido é cobrado pela API do X."""
    if not ENABLE_X_SEARCH or not X_BEARER_TOKEN or not mint:
        return None
    cached = _x_ca_cache.get(mint)
    if cached and time.time() - cached[0] < X_CACHE_SECONDS:
        return cached[1]
    data = await get_json(
        "https://api.x.com/2/tweets/search/recent",
        headers={"Authorization": f"Bearer {X_BEARER_TOKEN}"},
        params={"query": f'"{mint}" -is:retweet', "max_results": "10"},
    )
    if not isinstance(data, dict) or "meta" not in data:
        return None
    count = int(data["meta"].get("result_count", 0))
    _x_ca_cache[mint] = (time.time(), count)
    if len(_x_ca_cache) > 2000:
        for k in sorted(_x_ca_cache, key=lambda k: _x_ca_cache[k][0])[:500]:
            _x_ca_cache.pop(k, None)
    return count


# ==========================================
# Configuração (.env)
# ==========================================
def _bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "sim")

def _num(name: str, default: float, cast=float, minimum=None):
    try:
        v = cast(os.getenv(name, str(default)))
    except ValueError:
        v = cast(default)
    return max(minimum, v) if minimum is not None else v

# Bot do Telegram do caçador. Se não informar um token próprio, usa o do radar só para ENVIAR alertas
# (sem comandos), porque dois bots com o mesmo token não podem receber comandos ao mesmo tempo.
HUNTER_TOKEN = os.getenv("HUNTER_TELEGRAM_BOT_TOKEN", "").strip()
BOT_TOKEN = HUNTER_TOKEN or TELEGRAM_BOT_TOKEN
COMMANDS_ENABLED = bool(HUNTER_TOKEN)
CHAT_ID = os.getenv("HUNTER_TELEGRAM_CHAT_ID", "").strip() or TELEGRAM_CHAT_ID
# Chat separado só para tokens descobertos por CA postado no X (grupo/canal onde o bot esteja). Vazio = tudo no chat principal.
X_CHAT_ID = os.getenv("HUNTER_X_CHAT_ID", "").strip()
X_ALSO_MAIN = os.getenv("HUNTER_X_ALSO_MAIN", "false").strip().lower() in ("1", "true", "yes", "sim")

MIN_SCORE = int(_num("HUNTER_MIN_SCORE", 50, int, 0))                 # score mínimo de potencial para alertar
MAX_AGE_MIN = _num("HUNTER_MAX_AGE_MIN", 45, float, 1)                # só alerta tokens com até X minutos de vida
MAX_MCAP_USD = _num("HUNTER_MAX_MCAP_USD", 40000, float, 1000)        # acima disso já correu demais para ser "cedo"
MIN_BUYS_5M = int(_num("HUNTER_MIN_BUYS_5M", 8, int, 0))              # tração mínima (compras nos últimos 5 min)
ALERT_WITHOUT_TRACTION = _bool("HUNTER_ALERT_WITHOUT_TRACTION", False)
CHECK_SECONDS = _num("HUNTER_CHECK_SECONDS", 30, int, 10)
META_WORKERS = int(_num("HUNTER_META_WORKERS", 6, int, 1))
MAX_ALERTS_PER_HOUR = int(_num("HUNTER_MAX_ALERTS_PER_HOUR", 12, int, 1))
MILESTONES = _bool("HUNTER_MILESTONES", True)

# Descoberta pelo X: perfis que acabaram de postar o CA (jogos, launchpads, projetos...).
# Precisa de ENABLE_X_SEARCH=true e X_BEARER_TOKEN. A API do X cobra por post lido, por isso há um orçamento diário.
X_DISCOVERY = _bool("HUNTER_X_DISCOVERY", True)
X_POLL_SECONDS = int(_num("HUNTER_X_POLL_SECONDS", 180, int, 30))
X_MAX_RESULTS = int(_num("HUNTER_X_MAX_RESULTS", 20, int, 10))
X_DAILY_BUDGET = int(_num("HUNTER_X_DAILY_POST_BUDGET", 300, int, 10))   # posts lidos por dia (UTC)
X_MAX_POST_AGE_MIN = _num("HUNTER_X_MAX_POST_AGE_MIN", 30, float, 1)     # só posts recentes
X_MAX_TOKEN_AGE_MIN = _num("HUNTER_X_MAX_TOKEN_AGE_MIN", 120, float, 1)  # só tokens novos
X_WAIT_NO_PAIR_MIN = _num("HUNTER_X_WAIT_NO_PAIR_MIN", 10, float, 1)     # espera o token aparecer no DexScreener
X_PACING = _bool("HUNTER_X_PACING", True)                                # distribui o orçamento ao longo do dia (UTC)
X_PACE_BURST = int(_num("HUNTER_X_PACE_BURST", 20, int, 10))             # posts liberados logo na virada do dia
X_WATCH_ONLY = _bool("HUNTER_X_WATCH_ONLY", False)                       # lê só os perfis de HUNTER_X_WATCH (bem mais barato)
# Alertas dos POSTS de contas específicas (avisa cada post novo, não só quando há CA).
# Padrão: Pump.fun (@pumpdotfun), Elon Musk, kimchi1x e Alon (@a1lon9). Defina HUNTER_X_POST_ALERTS= (vazio) para desligar.
_posts_env = os.getenv("HUNTER_X_POST_ALERTS")
X_POST_ACCOUNTS = [h.strip().lstrip("@") for h in (_posts_env if _posts_env is not None else "pumpdotfun,elonmusk,kimchi1x,a1lon9").split(",") if h.strip()]
X_POSTS_POLL_SECONDS = int(_num("HUNTER_X_POSTS_POLL_SECONDS", 120, int, 30))
X_POSTS_BUDGET = int(_num("HUNTER_X_POSTS_DAILY_BUDGET", 150, int, 10))   # posts lidos por dia (UTC) só para este recurso
X_POSTS_MAX_AGE_MIN = _num("HUNTER_X_POSTS_MAX_AGE_MIN", 30, float, 1)     # ignora posts mais velhos (evita enxurrada ao ligar)
X_POSTS_CHAT_ID = os.getenv("HUNTER_X_POSTS_CHAT_ID", "").strip()          # vazio = chat principal
X_MIN_BUYS_5M = int(_num("HUNTER_X_MIN_BUYS_5M", 3, int, 0))             # tração mínima para tokens vindos do X
# Perfis cujos posts sempre são lidos (separe por vírgula, sem @). Pega lançamentos em que o post traz só o endereço.
X_WATCH = [h.strip().lstrip("@") for h in os.getenv("HUNTER_X_WATCH", "").split(",") if h.strip()]

def _build_x_query() -> str:
    custom = os.getenv("HUNTER_X_QUERY", "").strip()
    if custom:
        return custom
    if X_WATCH_ONLY and X_WATCH:
        handles = []
        for h in X_WATCH:
            if len(" OR ".join(f"from:{x}" for x in handles + [h])) > 400:
                break
            handles.append(h)
        return "(" + " OR ".join(f"from:{x}" for x in handles) + ") -is:retweet -is:reply"
    watch = ""
    if X_WATCH:
        handles = []
        for h in X_WATCH:
            if len(" OR ".join(f"from:{x}" for x in handles + [h])) > 230:   # limite de 512 caracteres da consulta
                break
            handles.append(h)
        watch = " OR (" + " OR ".join(f"from:{x}" for x in handles) + ")"
    return ('(("CA:" OR "CA :" OR "contract address" OR "CA is") (solana OR sol OR pump OR pumpfun OR launchpad OR game)'
            f' OR url:"pump.fun/coin"{watch}) -is:retweet -is:reply')

X_QUERY = _build_x_query()
TRACK_HOURS = _num("HUNTER_TRACK_HOURS", 6, float, 0.5)
REPORT_HOURS = _num("HUNTER_REPORT_HOURS", 6, float, 0)               # 0 desliga o resumo periódico
def _state_path(env_name: str, default: str) -> str:
    p = Path(os.getenv(env_name, default))
    return str(p if p.is_absolute() else BASE_DIR / p)   # sempre ao lado do script, não importa de onde você abre o terminal

# Alertas também para o público: usa a lista de inscritos do radar (quem enviou /start para o bot do radar).
# O Caçador só LÊ esse arquivo; quem grava é o radar. Se o radar roda em outra pasta, informe o caminho completo.
BROADCAST = _bool("HUNTER_BROADCAST", True)
SUBSCRIBERS_FILE = _state_path("HUNTER_SUBSCRIBERS_FILE", os.getenv("SUBSCRIBERS_FILE", "subscribers.json"))
SEEN_FILE = _state_path("HUNTER_SEEN_FILE", "hunter_seen.json")
ALERTS_FILE = _state_path("HUNTER_ALERTS_FILE", "hunter_alerts.json")
X_STATE_FILE = _state_path("HUNTER_X_STATE_FILE", "hunter_x_state.json")


# ==========================================
# Classificação de narrativa
# ==========================================
def _extra(name: str) -> list[str]:
    return [w.strip().lower() for w in os.getenv(name, "").split(",") if w.strip()]

MEME_WORDS = [
    "pepe", "doge", "shiba", "shib", "wojak", "chad", "gigachad", "cat", "cats", "kitty", "kitten", "dog", "puppy",
    "frog", "bonk", "cheems", "capybara", "hamster", "penguin", "pengu", "popcat", "nyan", "brainrot", "skibidi",
    "sigma", "rizz", "meme", "memecoin", "moon", "pump", "based", "degen", "ape", "monkey", "bear", "goat", "duck",
    "baby", "banana", "pizza", "dino", "alien", "ghost", "lol", "lmao", "bruh", "npc", "yeet", "67",
] + _extra("HUNTER_MEME_EXTRA")

GAME_WORDS = [
    "game", "games", "gaming", "gamer", "play", "player", "p2e", "play to earn", "rpg", "arcade", "quest", "esports",
    "minecraft", "roblox", "fortnite", "pokemon", "pokémon", "mario", "zelda", "gta", "call of duty", "steam",
    "metaverse", "idle", "clicker", "mmo", "battle royale", "tycoon", "hunter", "dungeon", "guild", "raid", "level up",
] + _extra("HUNTER_GAME_EXTRA")

CELEB_WORDS = [
    "trump", "elon", "musk", "kanye", "drake", "snoop", "taylor swift", "beyonce", "rihanna", "kardashian",
    "mrbeast", "mr beast", "andrew tate", "tate", "ronaldo", "messi", "neymar", "lebron", "pewdiepie", "logan paul",
    "jake paul", "ishowspeed", "melania", "barron", "vitalik", "sam altman", "zuckerberg", "bezos", "biden", "obama",
    "putin", "xqc", "kai cenat", "kaicenat", "adin ross", "milei", "lula", "bolsonaro", "cz", "saylor", "pope",
] + _extra("HUNTER_CELEB_EXTRA")

TREND_WORDS = [
    "ai", "agent", "agents", "gpt", "chatgpt", "grok", "claude", "openai", "llm", "deepseek", "robot", "robotics",
    "tesla", "spacex", "nvidia", "apple", "tariff", "election", "olympics", "world cup", "nasa", "mars", "bitcoin",
] + _extra("HUNTER_TREND_EXTRA")

BET_WORDS = [
    "bet", "bets", "betting", "bookie", "bookmaker", "casino", "gambling", "gamble", "sportsbook", "sports",
    "prediction market", "poker", "lottery", "jackpot", "slots", "wager", "parlay", "odds",
] + _extra("HUNTER_BET_EXTRA")

LAUNCHPAD_WORDS = [
    "launchpad", "launch pad", "fair launch", "presale", "ido", "bonk.fun", "letsbonk", "launchlab",
    "moonshot", "launched on", "now live", "just launched",
] + _extra("HUNTER_LAUNCHPAD_EXTRA")

def _compile(words: list[str]) -> re.Pattern:
    alt = "|".join(re.escape(w) for w in sorted(set(words), key=len, reverse=True))
    return re.compile(rf"(?<![a-z0-9])(?:{alt})(?![a-z0-9])", re.IGNORECASE)

TAG_PATTERNS = {
    "meme": _compile(MEME_WORDS),
    "game": _compile(GAME_WORDS),
    "celeb": _compile(CELEB_WORDS),
    "trend": _compile(TREND_WORDS),
    "launchpad": _compile(LAUNCHPAD_WORDS),
    "bet": _compile(BET_WORDS),
}
TAG_WEIGHTS = {"meme": 8, "game": 10, "celeb": 12, "trend": 6, "launchpad": 6, "bet": 10, "post_x": 8, "community": 3}
TAG_LABELS = {
    "meme": "🐸 Meme viral",
    "game": "🎮 Jogo",
    "celeb": "⭐ Nome de famoso",
    "trend": "📈 Tema em alta",
    "launchpad": "🚀 Launchpad",
    "bet": "🎰 Apostas/esportes",
    "post_x": "🔗 Link de post no X",
    "community": "👥 Comunidade no X",
}

def detect_tags(text: str) -> list[str]:
    text = (text or "").lower()
    return [tag for tag, pat in TAG_PATTERNS.items() if pat.search(text)]


# ==========================================
# Links (X, site, Telegram)
# ==========================================
X_URL_RE = re.compile(r"(?:https?://)?(?:www\.|mobile\.)?(?:x|twitter)\.com/([A-Za-z0-9_]+)(?:/(?:status|statuses)/(\d+))?", re.I)
X_RESERVED = {"home", "share", "intent", "search", "hashtag", "explore", "login", "i"}
BAD_SITE_HOSTS = (
    "pump.fun", "ipfs.io", "t.me", "telegram.me", "x.com", "twitter.com", "discord.gg", "discord.com",
    "youtube.com", "youtu.be", "tiktok.com", "instagram.com", "dexscreener.com", "dextools.io",
)

def parse_x_link(url: str) -> Optional[dict]:
    """Reconhece perfil, post (status) ou comunidade do X/Twitter."""
    url = (url or "").strip()
    if not url:
        return None
    if re.search(r"(?:x|twitter)\.com/i/communities/\d+", url, re.I):
        return {"kind": "community", "user": None, "status_id": None}
    m = X_URL_RE.search(url)
    if not m or m.group(1).lower() in X_RESERVED:
        return None
    return {"kind": "status" if m.group(2) else "profile", "user": m.group(1), "status_id": m.group(2)}

def valid_website(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if not url.lower().startswith(("http://", "https://")):
        url = "https://" + url
    host = (urlparse(url).netloc or "").lower().split(":")[0]
    if "." not in host or any(host == b or host.endswith("." + b) for b in BAD_SITE_HOSTS):
        return ""
    return url


# ==========================================
# Estado
# ==========================================
@dataclass
class HCand:
    mint: str
    name: str
    symbol: str
    uri: str
    dev: str
    initial_buy: Optional[float]
    created_at: float
    description: str = ""
    x_url: str = ""
    x_user: Optional[str] = None
    x_kind: str = ""
    x_status_id: Optional[str] = None
    website: str = ""
    telegram: str = ""
    tags: list = field(default_factory=list)
    meta_done: bool = False
    meta_fail: int = 0
    first_mcap: Optional[float] = None
    last_score: int = 0
    last_block: str = ""
    deep_at: float = 0.0
    alerted: bool = False
    source: str = "pump"                 # "pump" (lançamento em tempo real) ou "x" (CA postado no X)
    xi: Optional[dict] = None            # dados do X já conhecidos (seguidores, idade da conta, curtidas)
    x_authors: list = field(default_factory=list)
    post_text: str = ""
    bio: str = ""
    pair_seen: bool = False
    token_age_min: Optional[float] = None
    tags_named: bool = False

pool: dict[str, HCand] = {}
hunter_seen: list[str] = []
alert_records: list[dict] = []
alert_times: deque = deque()
meta_queue: Optional[asyncio.Queue] = None
public_bot: Optional[Bot] = None      # bot usado para falar com os inscritos (o mesmo token do radar)
_blocked: set[str] = set()            # inscritos que bloquearam o bot (não insiste neles)
_x_skip: set[str] = set()             # endereços vindos do X já descartados (velhos, sem token, etc.)
_x_state: dict = {"day": "", "used": 0, "since_id": None, "fails": 0, "budget_warned": False, "fail_warned": False}
_x_posts_state: dict = {"day": "", "used": 0, "since_id": None, "fails": 0, "budget_warned": False, "fail_warned": False}
_bg_tasks: set = set()
_x_user_cache: dict[str, tuple[float, dict]] = {}

def load_state():
    global hunter_seen, alert_records
    for path, setter in ((SEEN_FILE, "seen"), (ALERTS_FILE, "alerts")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            data = []
        if setter == "seen":
            hunter_seen = data if isinstance(data, list) else []
        else:
            alert_records = data if isinstance(data, list) else []

def load_x_state():
    try:
        with open(X_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            _x_state.update({k: data[k] for k in ("day", "used", "since_id") if k in data})
            posts = data.get("posts")
            if isinstance(posts, dict):
                _x_posts_state.update({k: posts[k] for k in ("day", "used", "since_id") if k in posts})
    except (FileNotFoundError, json.JSONDecodeError):
        pass

def save_x_state():
    try:
        with open(X_STATE_FILE, "w", encoding="utf-8") as f:
            data = {k: _x_state[k] for k in ("day", "used", "since_id")}
            data["posts"] = {k: _x_posts_state[k] for k in ("day", "used", "since_id")}
            json.dump(data, f)
    except OSError as e:
        log.warning("Falha ao salvar %s: %s", X_STATE_FILE, e)

def save_seen():
    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            json.dump(hunter_seen[-5000:], f)
    except OSError as e:
        log.warning("Falha ao salvar %s: %s", SEEN_FILE, e)

def save_alerts():
    try:
        with open(ALERTS_FILE, "w", encoding="utf-8") as f:
            json.dump(alert_records[-500:], f, indent=1)
    except OSError as e:
        log.warning("Falha ao salvar %s: %s", ALERTS_FILE, e)


# ==========================================
# Utilidades
# ==========================================
def fnum(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default

def usd(n: Optional[float]) -> str:
    if n is None:
        return "N/D"
    n = float(n)
    if n >= 1_000_000:
        return f"${n / 1_000_000:.2f}M"
    if n >= 1_000:
        return f"${n / 1_000:.1f}K"
    return f"${n:.0f}"

def esc(s) -> str:
    return html.escape(str(s or ""), quote=False)

def load_public_subscribers() -> list[str]:
    """Lê (sem gravar) os inscritos do radar. Falhas de leitura não derrubam o bot."""
    try:
        with open(SUBSCRIBERS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return []
    except (json.JSONDecodeError, OSError) as e:
        log.warning("Não consegui ler %s: %s", SUBSCRIBERS_FILE, e)
        return []
    ids = list(data.keys()) if isinstance(data, dict) else []
    admin = str(CHAT_ID)
    return [c for c in ids if c != admin and c not in _blocked]

async def broadcast_public(text: str) -> int:
    if not BROADCAST or public_bot is None:
        return 0
    delivered = 0
    for chat_id in load_public_subscribers():
        for _ in range(2):
            try:
                await public_bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", disable_web_page_preview=True)
                delivered += 1
                break
            except RetryAfter as e:
                await asyncio.sleep(float(e.retry_after) + 1)
            except Forbidden:
                _blocked.add(chat_id)   # o radar remove esse inscrito da lista quando ele também falhar lá
                break
            except BadRequest as e:
                if "chat not found" in str(e).lower():
                    _blocked.add(chat_id)
                else:
                    log.warning("Falha ao enviar para %s: %s", chat_id, e)
                break
            except Exception as e:
                log.warning("Falha ao enviar para %s: %s", chat_id, e)
                break
        await asyncio.sleep(0.05)  # ~20 mensagens/s, abaixo do limite do Telegram
    return delivered

async def _send_to(bot, chat_id: str, text: str) -> bool:
    try:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", disable_web_page_preview=True)
        return True
    except Exception as e:
        log.warning("Falha ao enviar mensagem para o chat %s: %s", chat_id, e)
        return False

async def notify(bot, text: str, public: bool = False, source: str = ""):
    """
    Envia ao administrador; com public=True também aos inscritos do radar (em segundo plano).
    Alertas de origem "x" (CA postado no X) vão para HUNTER_X_CHAT_ID, se configurado.
    """
    if source == "x" and X_CHAT_ID:
        dest = X_CHAT_ID
    elif source == "xposts" and X_POSTS_CHAT_ID:
        dest = X_POSTS_CHAT_ID
    else:
        dest = CHAT_ID
    if not dest:
        log.warning("HUNTER_TELEGRAM_CHAT_ID/TELEGRAM_CHAT_ID não configurado; mensagem não enviada.")
        return
    await _send_to(bot, dest, text)
    if dest != CHAT_ID and X_ALSO_MAIN and CHAT_ID:
        await _send_to(bot, CHAT_ID, text)
    if public and BROADCAST and public_bot is not None:
        task = asyncio.create_task(broadcast_public(text))
        _bg_tasks.add(task)
        task.add_done_callback(_bg_tasks.discard)


# ==========================================
# Metadados do lançamento (X, site, Telegram)
# ==========================================
async def fetch_metadata(uri: str) -> Optional[dict]:
    urls = [uri]
    m = re.search(r"/ipfs/([A-Za-z0-9]+)", uri or "")
    if m:
        cid = m.group(1)
        urls += [f"https://ipfs.io/ipfs/{cid}", f"https://dweb.link/ipfs/{cid}", f"https://gateway.pinata.cloud/ipfs/{cid}"]
    seen_urls = set()
    for u in urls:
        if not u or u in seen_urls:
            continue
        seen_urls.add(u)
        data = await get_json(u, timeout_sec=6)
        if isinstance(data, dict):
            return data
    return None

def apply_metadata(c: HCand, meta: dict):
    c.description = str(meta.get("description") or "")[:500]

    tw = str(meta.get("twitter") or "").strip()
    px = parse_x_link(tw)
    if not px:  # algumas pessoas colocam o link do X no campo do site
        alt = str(meta.get("website") or "").strip()
        px = parse_x_link(alt)
        tw = alt if px else ""
    if px:
        c.x_url = tw if tw.lower().startswith("http") else "https://" + tw
        c.x_user, c.x_kind, c.x_status_id = px["user"], px["kind"], px["status_id"]

    c.website = valid_website(meta.get("website"))
    tg = str(meta.get("telegram") or "").strip()
    if "t.me" in tg.lower() or "telegram" in tg.lower():
        c.telegram = tg

    tags = set(detect_tags(f"{c.name} {c.symbol} {c.description}"))
    if c.x_kind == "status":
        tags.add("post_x")
    elif c.x_kind == "community":
        tags.add("community")
    c.tags = sorted(tags)
    c.meta_done = True

async def meta_worker():
    loop = asyncio.get_running_loop()
    while True:
        mint = await meta_queue.get()
        c = pool.get(mint)
        if not c or c.meta_done:
            continue
        meta = await fetch_metadata(c.uri) if c.uri else None
        if meta:
            apply_metadata(c, meta)
        else:
            c.meta_fail += 1
            if not c.uri or c.meta_fail >= 3:
                c.meta_done = True  # segue só com nome e símbolo
            else:
                loop.call_later(8, lambda m=mint: meta_queue.put_nowait(m) if not meta_queue.full() else None)


# ==========================================
# Descoberta em tempo real (PumpPortal)
# ==========================================
async def listener():
    log.info("Escutando novos tokens da Pump.fun em tempo real (%s)...", PUMPPORTAL_WS_URL)
    while True:
        try:
            async with websockets.connect(PUMPPORTAL_WS_URL, ping_interval=20, close_timeout=10) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                log.info("Inscrito em novos tokens do PumpPortal.")
                async for raw in ws:
                    try:
                        data = json.loads(raw)
                    except ValueError:
                        continue
                    mint = data.get("mint")
                    if not mint or mint in pool or mint in hunter_seen:
                        continue
                    ib = data.get("initialBuy")
                    c = HCand(
                        mint=mint,
                        name=str(data.get("name") or ""),
                        symbol=str(data.get("symbol") or ""),
                        uri=str(data.get("uri") or ""),
                        dev=str(data.get("traderPublicKey") or ""),
                        initial_buy=fnum(ib) if ib is not None else None,
                        created_at=time.time(),
                    )
                    c.tags = detect_tags(f"{c.name} {c.symbol}")
                    pool[mint] = c
                    if len(pool) > 8000:
                        for old in sorted(pool, key=lambda k: pool[k].created_at)[:1000]:
                            pool.pop(old, None)
                    if meta_queue is not None and not meta_queue.full():
                        meta_queue.put_nowait(mint)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("PumpPortal desconectado (%s). Reconectando em 5s...", e)
            await asyncio.sleep(5)


# ==========================================
# Dados de mercado (DexScreener em lote: até 30 tokens por chamada)
# ==========================================
async def fetch_pairs(mints: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for i in range(0, len(mints), 30):
        chunk = mints[i:i + 30]
        data = await get_json(f"{DEX_BASE}/tokens/v1/solana/{','.join(chunk)}")
        pairs = data if isinstance(data, list) else (data.get("pairs") if isinstance(data, dict) else None)
        for p in pairs or []:
            addr = (p.get("baseToken") or {}).get("address")
            if addr not in chunk:
                continue
            weight = fnum((p.get("liquidity") or {}).get("usd")) + fnum((p.get("volume") or {}).get("h1"))
            if addr not in out or weight > out[addr]["_w"]:
                p["_w"] = weight
                out[addr] = p
        await asyncio.sleep(0.3)
    return out

def market_numbers(pair: Optional[dict]) -> Optional[dict]:
    if not pair:
        return None
    tx = (pair.get("txns") or {}).get("m5") or {}
    return {
        "buys": int(fnum(tx.get("buys"))),
        "sells": int(fnum(tx.get("sells"))),
        "vol5": fnum((pair.get("volume") or {}).get("m5")),
        "mcap": fnum(pair.get("marketCap") or pair.get("fdv")),
        "price": fnum(pair.get("priceUsd")),
    }


# ==========================================
# X (opcional, pago por post lido): conta, seguidores e CA
# ==========================================
async def x_enrich(c: HCand) -> Optional[dict]:
    if not (ENABLE_X_SEARCH and X_BEARER_TOKEN):
        return None
    headers = {"Authorization": f"Bearer {X_BEARER_TOKEN}"}
    info: dict = {}
    if c.x_user:
        key = c.x_user.lower()
        cached = _x_user_cache.get(key)
        if cached and time.time() - cached[0] < 3600:
            info.update(cached[1])
        else:
            data = await get_json(
                f"https://api.x.com/2/users/by/username/{c.x_user}", headers=headers,
                params={"user.fields": "public_metrics,created_at,verified"},
            )
            d = data.get("data") if isinstance(data, dict) else None
            if d:
                pm = d.get("public_metrics") or {}
                days = None
                try:
                    created = datetime.fromisoformat(str(d.get("created_at")).replace("Z", "+00:00"))
                    days = (datetime.now(timezone.utc) - created).total_seconds() / 86400
                except ValueError:
                    pass
                u = {"followers": int(fnum(pm.get("followers_count"))), "created_days": days, "verified": bool(d.get("verified"))}
                _x_user_cache[key] = (time.time(), u)
                info.update(u)
    if c.x_status_id:
        data = await get_json(
            f"https://api.x.com/2/tweets/{c.x_status_id}", headers=headers, params={"tweet.fields": "public_metrics"},
        )
        d = data.get("data") if isinstance(data, dict) else None
        if d:
            info["post_likes"] = int(fnum((d.get("public_metrics") or {}).get("like_count")))
    info["ca_mentions"] = await x_ca_mentions(c.mint)
    return info or None


# ==========================================
# Score de POTENCIAL (0-100)
# ==========================================
def score_candidate(c: HCand, mk: Optional[dict], xi: Optional[dict] = None, sec=None):
    reasons: list[str] = []
    risks: list[str] = []

    # A) Presença social (até 35)
    s = 0
    if c.x_url:
        s += 15
        reasons.append(f"Tem X{f' (@{c.x_user})' if c.x_user else ''}")
    if c.website:
        s += 12
        reasons.append("Tem site")
    if c.telegram:
        s += 4
        reasons.append("Tem Telegram")
    if c.x_url and c.website:
        s += 4
        reasons.append("X + site: presença completa")
    social = min(s, 35)
    if not c.x_url and not c.website:
        risks.append("Sem X e sem site (projeto anônimo)")

    # B) Narrativa (até 35)
    n = sum(TAG_WEIGHTS.get(t, 0) for t in c.tags)
    if c.source == "x":
        nauth = max(1, len(c.x_authors))
        if nauth >= 2:
            n += min(4 * (nauth - 1), 8)
        f0 = (xi or {}).get("followers")
        reasons.append(
            f"CA postado agora no X por @{c.x_user or '?'}"
            + (f" ({f0:,} seguidores)".replace(",", ".") if f0 else "")
            + (f" + {nauth - 1} outro(s) perfil(is)" if nauth > 1 else "")
        )
    for t in c.tags:
        reasons.append(f"Narrativa: {TAG_LABELS.get(t, t)}")
    if xi:
        f = xi.get("followers") or 0
        if f >= 100_000:
            n += 10
            reasons.append(f"Conta do X grande ({f:,} seguidores)".replace(",", "."))
        elif f >= 20_000:
            n += 5
            reasons.append(f"Conta do X com {f:,} seguidores".replace(",", "."))
        if (xi.get("post_likes") or 0) >= 1000:
            n += 5
            reasons.append(f"Post linkado com {xi['post_likes']} curtidas")
        if (xi.get("ca_mentions") or 0) >= 1:
            n += 4
            reasons.append(f"CA já circula no X ({xi['ca_mentions']} posts)")
        days = xi.get("created_days")
        if days is not None and days < 7:
            risks.append(f"Conta do X criada há {days:.0f} dia(s)")
        if f and f < 100:
            risks.append("Conta do X com poucos seguidores")
    narrative = min(n, 35)
    if "celeb" in c.tags:
        risks.append("Nome de famoso no token: golpes costumam usar isso (confira se há ligação real)")

    # C) Tração / momentum (até 25)
    m = 0
    if mk:
        b, sl, v5, mcap = mk["buys"], mk["sells"], mk["vol5"], mk["mcap"]
        if b >= 8: m += 4
        if b >= 20: m += 4
        if b >= 40: m += 3
        if b + sl >= 10 and b / (b + sl) >= 0.65:
            m += 4
            reasons.append(f"Pressão compradora ({b} compras x {sl} vendas em 5 min)")
        if v5 >= 1500: m += 4
        if v5 >= 5000: m += 3
        if 4000 <= mcap <= 25000: m += 3
        if b + sl >= 10 and sl > b:
            risks.append("Mais vendas que compras nos últimos 5 min")
    else:
        risks.append("Sem dados de mercado ainda (token pode não estar indexado)")
    momentum = min(m, 25)

    # D) Penalidades de risco
    pen = 0
    if c.initial_buy and 0 < c.initial_buy <= 1e9:
        dev_pct = c.initial_buy / 1e9 * 100
        if dev_pct >= 15:
            pen += 4
            risks.append(f"Dev comprou ≈{dev_pct:.0f}% do supply no lançamento")
    if sec is not None:
        if sec.bundled_detected:
            pen += 8
            risks.append(f"Suspeita de multi-carteiras ({sec.bundled_wallets_count} carteiras, ~{sec.bundled_total_pct:.1f}% juntas)")
        if sec.max_single_holder_pct >= 30:
            pen += 8
            risks.append(f"Uma carteira concentra {sec.max_single_holder_pct:.1f}% do supply")
        elif sec.max_single_holder_pct >= 20:
            pen += 4
            risks.append(f"Carteira grande com {sec.max_single_holder_pct:.1f}% do supply")
        if sec.top10_holders_pct >= 60:
            pen += 4
            risks.append(f"Top 10 carteiras com {sec.top10_holders_pct:.1f}%")
    if xi:
        days = xi.get("created_days")
        if days is not None and days < 7:
            pen += 5

    total = max(0, min(100, social + narrative + momentum - pen))
    parts = {"social": social, "narrativa": narrative, "tracao": momentum, "penalidades": pen}
    return total, reasons, risks, parts

def score_label(score: int) -> str:
    if score >= 75:
        return "🔥 MUITO FORTE"
    if score >= 60:
        return "💪 FORTE"
    return "👀 PROMISSOR"


# ==========================================
# Mensagem de alerta
# ==========================================
def build_alert(c: HCand, score: int, reasons: list[str], risks: list[str], mk: Optional[dict], parts: dict) -> str:
    age_min = c.token_age_min if c.token_age_min is not None else (time.time() - c.created_at) / 60
    tags = " • ".join(TAG_LABELS[t] for t in c.tags if t in TAG_LABELS) or "—"
    x = f"✅ @{esc(c.x_user)}" if c.x_user else ("✅" if c.x_url else "❌")
    lines = [
        "🏹 <b>CAÇADOR — MOEDA NOVA</b>",
        "",
        f"<b>{esc(c.name)}</b> (${esc(c.symbol)})",
        f"⏱ Lançada há <b>{age_min:.0f} min</b> | 🎯 Potencial: <b>{score}/100</b> {score_label(score)}",
        f"🏷 {tags}",
        f"🐦 X: {x} | 🌐 Site: {'✅' if c.website else '❌'} | ✈️ Telegram: {'✅' if c.telegram else '❌'}",
    ]
    if c.source == "x" and c.x_user:
        fol = (c.xi or {}).get("followers")
        others = f" + {len(c.x_authors) - 1} outro(s)" if len(c.x_authors) > 1 else ""
        lines.insert(5, f"📣 CA postado no X por <b>@{esc(c.x_user)}</b>"
                        + (f" ({fol:,} seguidores)".replace(",", ".") if fol else "") + others
                        + (f' — <a href="{esc(c.x_url)}">ver post</a>' if c.x_url else ""))
    if mk:
        mc = f"💰 Market cap: <b>{usd(mk['mcap'])}</b>"
        if c.first_mcap and c.first_mcap > 0 and mk["mcap"] > 0:
            mc += f" ({mk['mcap'] / c.first_mcap:.1f}x desde a 1ª leitura de {usd(c.first_mcap)})"
        lines.append(mc)
        lines.append(f"📈 5 min: 🟢 {mk['buys']} compras | 🔴 {mk['sells']} vendas | Vol {usd(mk['vol5'])}")
    else:
        lines.append("💰 Market cap: N/D (ainda sem dados no DexScreener)")
    if c.description:
        lines += ["", f"📝 <i>{esc(c.description[:160])}</i>"]
    lines += [
        "",
        f"<b>Pontos:</b> social {parts['social']}/35 • narrativa {parts['narrativa']}/35 • tração {parts['tracao']}/25"
        + (f" • riscos −{parts['penalidades']}" if parts["penalidades"] else ""),
    ]
    if reasons:
        lines += ["", "<b>Por que apareceu:</b>"] + [f"• {esc(r)}" for r in reasons[:8]]
    lines += ["", "<b>⚠️ Riscos:</b>"] + [f"• {esc(r)}" for r in risks[:8]] + [
        "• Token com minutos de vida: pode perder quase todo o valor em segundos (rug, dev vendendo, bots)."
    ]
    if mk and mk["mcap"] > 0:
        m = mk["mcap"]
        lines += [
            "",
            f"📌 <b>Referências (informativo, não é recomendação):</b> stop mental −30% ≈ {usd(m * 0.7)} • "
            f"realizar parte em 2x ≈ {usd(m * 2)} • 3x ≈ {usd(m * 3)}",
        ]
    lines += ["", f"🧾 <b>CA:</b> <code>{c.mint}</code>", ""]
    links = [
        f'<a href="https://pump.fun/coin/{c.mint}">Pump.fun</a>',
        f'<a href="https://dexscreener.com/solana/{c.mint}">DexScreener</a>',
        f'<a href="https://rugcheck.xyz/tokens/{c.mint}">Rugcheck</a>',
    ]
    if c.x_url:
        links.append(f'<a href="{esc(c.x_url)}">X</a>')
    if c.website:
        links.append(f'<a href="{esc(c.website)}">Site</a>')
    lines.append("🔗 " + " | ".join(links))
    return "\n".join(lines)


# ==========================================
# Avaliação contínua dos lançamentos
# ==========================================
# ==========================================
# Descoberta pelo X: perfis que acabaram de postar o CA
# ==========================================
ADDR_RE = re.compile(r"(?<![1-9A-HJ-NP-Za-km-z])[1-9A-HJ-NP-Za-km-z]{32,44}(?![1-9A-HJ-NP-Za-km-z])")
IGNORED_ADDRS = {
    "So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", "11111111111111111111111111111111",
    PUMP_PROGRAM_ID, RAYDIUM_AUTHORITY,
}

def x_diagnostic() -> str:
    """Mostra o que o bot enxergou do .env (sem revelar o token) para achar o motivo de o X não ligar."""
    env_path = BASE_DIR / ".env"
    raw = os.getenv("ENABLE_X_SEARCH")
    tok = X_BEARER_TOKEN
    return (
        f".env ao lado do script: {'encontrado' if env_path.exists() else 'NÃO encontrado (esperado em ' + str(env_path) + ')'} | "
        f"ENABLE_X_SEARCH={raw!r} -> {'ligado' if ENABLE_X_SEARCH else 'desligado (precisa ser true)'} | "
        f"X_BEARER_TOKEN: {('preenchido, ' + str(len(tok)) + ' caracteres') if tok else 'vazio ou ausente'}"
    )

def x_discovery_active() -> bool:
    return X_DISCOVERY and ENABLE_X_SEARCH and bool(X_BEARER_TOKEN)

def extract_addresses(text: str, urls: list[str]) -> list[str]:
    """Endereços (CA) citados no texto do post ou nos links (ex.: pump.fun/coin/<CA>)."""
    found: list[str] = []
    for src in [text or ""] + [u or "" for u in urls]:
        for a in ADDR_RE.findall(src):
            if a in IGNORED_ADDRS or a in found or not any(ch.isdigit() for ch in a):
                continue
            found.append(a)
    return found

def _iso_age(iso: Optional[str]) -> Optional[float]:
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except ValueError:
        return None

def handle_x_post(t: dict, user: Optional[dict]) -> int:
    """Transforma um post com CA em candidato. Retorna quantos candidatos novos criou."""
    age_s = _iso_age(t.get("created_at"))
    if age_s is not None and age_s > X_MAX_POST_AGE_MIN * 60:
        return 0
    text = str(t.get("text") or "")
    urls = [str(u.get("expanded_url") or u.get("url") or "") for u in ((t.get("entities") or {}).get("urls") or [])]
    addrs = extract_addresses(text, urls)
    if not addrs:
        return 0

    user = user or {}
    uname = user.get("username")
    pm = user.get("public_metrics") or {}
    followers = int(fnum(pm.get("followers_count")))
    acc_age = _iso_age(user.get("created_at"))
    xi = {
        "followers": followers,
        "created_days": (acc_age / 86400) if acc_age is not None else None,
        "verified": bool(user.get("verified")),
        "post_likes": int(fnum((t.get("public_metrics") or {}).get("like_count"))),
        "ca_mentions": 1,
    }
    bio = str(user.get("description") or "")[:200]
    site, telegram = "", ""
    prof_urls = [str(u.get("expanded_url") or "") for u in (((user.get("entities") or {}).get("url") or {}).get("urls") or [])]
    for u in urls + prof_urls:
        if not site:
            site = valid_website(u) if u and "t.me" not in u else ""
        if "t.me" in u and not telegram:
            telegram = u

    created = 0
    for addr in addrs[:3]:
        c = pool.get(addr)
        if c:
            if uname and uname not in c.x_authors:
                c.x_authors.append(uname)
                if c.xi is not None:
                    c.xi["ca_mentions"] = len(c.x_authors)
                    if followers > (c.xi.get("followers") or 0):   # destaca o perfil maior
                        c.xi.update({k: xi[k] for k in ("followers", "created_days", "verified", "post_likes")})
                        c.x_user = uname
            continue
        if addr in hunter_seen or addr in _x_skip:
            continue
        c = HCand(mint=addr, name="", symbol="", uri="", dev="", initial_buy=None, created_at=time.time(),
                  meta_done=True, source="x", xi=xi, post_text=text[:300], bio=bio)
        c.x_user = uname
        c.x_kind = "status"
        c.x_status_id = t.get("id")
        c.x_url = f"https://x.com/{uname}/status/{t.get('id')}" if uname else ""
        c.x_authors = [uname] if uname else []
        c.website, c.telegram = site, telegram
        c.tags = sorted(detect_tags(f"{text} {bio}"))
        pool[addr] = c
        created += 1
    return created

async def x_search_recent(params: dict):
    """Retorna (dados, erro). Mantém o motivo da falha para avisar o usuário."""
    try:
        async with session.get(
            "https://api.x.com/2/tweets/search/recent",
            headers={"Authorization": f"Bearer {X_BEARER_TOKEN}"}, params=params,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            body = await r.text()
            if r.status != 200:
                return None, f"HTTP {r.status}: {body[:160]}"
            return json.loads(body), ""
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

async def x_poll_once(bot) -> int:
    now_dt = _utc_now()
    today = now_dt.date().isoformat()
    if _x_state["day"] != today:
        _x_state.update({"day": today, "used": 0, "budget_warned": False})
    remaining = X_DAILY_BUDGET - int(_x_state["used"])
    if remaining < 10:  # 10 é o mínimo da API
        if not _x_state["budget_warned"]:
            _x_state["budget_warned"] = True
            await notify(bot, f"⏸ <b>Descoberta pelo X pausada</b>: orçamento diário de {X_DAILY_BUDGET} posts lidos atingido. "
                              "Volta amanhã (00:00 UTC). Ajuste com HUNTER_X_DAILY_POST_BUDGET.")
        return 0

    allowed = remaining
    if X_PACING:
        # libera o orçamento aos poucos durante o dia (UTC), em vez de gastar tudo nos primeiros minutos
        secs = now_dt.hour * 3600 + now_dt.minute * 60 + now_dt.second
        allowed = int(X_DAILY_BUDGET * secs / 86400) + X_PACE_BURST - int(_x_state["used"])
        if allowed < 10:
            return 0   # sem custo: espera o orçamento do dia liberar mais posts

    params = {
        "query": X_QUERY,
        "max_results": str(max(10, min(X_MAX_RESULTS, remaining, allowed, 100))),
        "tweet.fields": "created_at,public_metrics,entities,author_id",
        "expansions": "author_id",
        "user.fields": "username,public_metrics,created_at,verified,description,entities",
    }
    if _x_state.get("since_id"):
        params["since_id"] = str(_x_state["since_id"])   # só posts novos (evita pagar duas vezes pelo mesmo post)

    data, err = await x_search_recent(params)
    if data is None:
        _x_state["fails"] += 1
        log.warning("Busca no X falhou (%sx): %s", _x_state["fails"], err)
        if _x_state["fails"] >= 3 and not _x_state["fail_warned"]:
            _x_state["fail_warned"] = True
            await notify(bot, "⚠️ <b>Descoberta pelo X com problema</b>\n"
                              f"Motivo: <code>{esc(err[:200])}</code>\n"
                              "Confira o X_BEARER_TOKEN, os créditos da conta e a consulta (HUNTER_X_QUERY).")
        return 0
    _x_state["fails"] = 0
    _x_state["fail_warned"] = False

    posts = data.get("data") or []
    _x_state["used"] = int(_x_state["used"]) + len(posts)
    newest = (data.get("meta") or {}).get("newest_id")
    if newest:
        _x_state["since_id"] = newest
    users = {u.get("id"): u for u in ((data.get("includes") or {}).get("users") or [])}
    created = sum(handle_x_post(t, users.get(t.get("author_id"))) for t in posts)
    save_x_state()
    log.info("X: %s posts lidos (%s/%s hoje), %s novo(s) candidato(s).", len(posts), _x_state["used"], X_DAILY_BUDGET, created)
    return created

async def x_poll_loop(bot):
    if not X_DISCOVERY:
        return
    if not (ENABLE_X_SEARCH and X_BEARER_TOKEN):
        log.warning("Descoberta pelo X desligada: configure ENABLE_X_SEARCH=true e X_BEARER_TOKEN no .env.")
        log.warning("Diagnóstico: %s", x_diagnostic())
        return
    log.info("Descoberta pelo X ativa: consulta a cada %ss, orçamento de %s posts/dia (≈ US$ %.2f/dia a US$ 0,005 por post)%s%s.",
             X_POLL_SECONDS, X_DAILY_BUDGET, X_DAILY_BUDGET * 0.005,
             ", distribuído ao longo do dia" if X_PACING else "",
             ", só perfis vigiados" if (X_WATCH_ONLY and X_WATCH) else "")
    if X_WATCH_ONLY and not X_WATCH:
        log.warning("HUNTER_X_WATCH_ONLY=true, mas HUNTER_X_WATCH está vazio: usando a busca ampla.")
    log.info("Diagnóstico: %s", x_diagnostic())
    save_x_state()   # cria o hunter_x_state.json já na inicialização
    while True:
        try:
            await x_poll_once(bot)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Erro na descoberta pelo X")
        await asyncio.sleep(X_POLL_SECONDS)


def x_api_ready() -> bool:
    return ENABLE_X_SEARCH and bool(X_BEARER_TOKEN)

def build_post_alert(t: dict, user: Optional[dict]) -> str:
    user = user or {}
    uname = str(user.get("username") or "?")
    name = str(user.get("name") or "")
    age_s = _iso_age(t.get("created_at"))
    text = str(t.get("text") or "")
    when = f" — há {max(0, int(age_s // 60))} min" if age_s is not None else ""
    lines = [f"🐦 <b>@{esc(uname)}</b>" + (f" ({esc(name)})" if name else "") + when, "",
             esc(text[:700]) + ("…" if len(text) > 700 else "")]
    tags = [TAG_LABELS[x] for x in detect_tags(text) if x in TAG_LABELS]
    if tags:
        lines += ["", "🏷 " + " • ".join(tags)]
    urls = [str(u.get("expanded_url") or u.get("url") or "") for u in ((t.get("entities") or {}).get("urls") or [])]
    addrs = extract_addresses(text, urls)
    if addrs:
        lines += ["", "🧾 CA no post: " + " | ".join(f"<code>{a}</code>" for a in addrs[:2]),
                  "<i>O Caçador vai avaliar esse token. Posts de contas grandes também são usados por golpistas quando a conta é invadida.</i>"]
    lines += ["", f'🔗 <a href="https://x.com/{esc(uname)}/status/{esc(t.get("id"))}">ver post</a>']
    return "\n".join(lines)

async def x_posts_poll_once(bot) -> int:
    """Avisa cada post novo (sem retweets/respostas) das contas em HUNTER_X_POST_ALERTS."""
    st = _x_posts_state
    today = _utc_now().date().isoformat()
    if st["day"] != today:
        st.update({"day": today, "used": 0, "budget_warned": False})
    remaining = X_POSTS_BUDGET - int(st["used"])
    if remaining < 10:  # 10 é o mínimo da API
        if not st["budget_warned"]:
            st["budget_warned"] = True
            await notify(bot, f"⏸ <b>Alertas de posts do X pausados</b>: orçamento diário de {X_POSTS_BUDGET} posts lidos atingido. "
                              "Volta amanhã (00:00 UTC). Ajuste com HUNTER_X_POSTS_DAILY_BUDGET.", source="xposts")
        return 0

    query = "(" + " OR ".join(f"from:{a}" for a in X_POST_ACCOUNTS) + ") -is:retweet -is:reply"
    params = {
        "query": query,
        "max_results": str(max(10, min(20, remaining, 100))),
        "tweet.fields": "created_at,public_metrics,entities,author_id",
        "expansions": "author_id",
        "user.fields": "username,name,public_metrics,created_at,verified,description,entities",
    }
    if st.get("since_id"):
        params["since_id"] = str(st["since_id"])

    data, err = await x_search_recent(params)
    if data is None:
        st["fails"] += 1
        log.warning("Busca dos posts vigiados no X falhou (%sx): %s", st["fails"], err)
        if st["fails"] >= 3 and not st["fail_warned"]:
            st["fail_warned"] = True
            await notify(bot, "⚠️ <b>Alertas de posts do X com problema</b>\n"
                              f"Motivo: <code>{esc(err[:200])}</code>", source="xposts")
        return 0
    st["fails"] = 0
    st["fail_warned"] = False

    posts = data.get("data") or []
    st["used"] = int(st["used"]) + len(posts)
    newest = (data.get("meta") or {}).get("newest_id")
    if newest:
        st["since_id"] = newest
    users = {u.get("id"): u for u in ((data.get("includes") or {}).get("users") or [])}

    sent = 0
    for t in reversed(posts):   # do mais antigo para o mais novo
        age_s = _iso_age(t.get("created_at"))
        if age_s is not None and age_s > X_POSTS_MAX_AGE_MIN * 60:
            continue
        user = users.get(t.get("author_id"))
        await notify(bot, build_post_alert(t, user), source="xposts")
        sent += 1
        handle_x_post(t, user)   # se o post traz um CA, o token entra na fila de avaliação
    save_x_state()
    log.info("X (contas vigiadas): %s post(s) lido(s) (%s/%s hoje), %s alerta(s) enviado(s).", len(posts), st["used"], X_POSTS_BUDGET, sent)
    return sent

async def x_posts_loop(bot):
    if not X_POST_ACCOUNTS:
        return
    if not x_api_ready():
        log.warning("Alertas de posts do X desligados: configure ENABLE_X_SEARCH=true e X_BEARER_TOKEN no .env.")
        return
    log.info("Alertas de posts do X ativos para: %s (consulta a cada %ss, orçamento de %s posts/dia).",
             ", ".join("@" + a for a in X_POST_ACCOUNTS), X_POSTS_POLL_SECONDS, X_POSTS_BUDGET)
    while True:
        try:
            await x_posts_poll_once(bot)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Erro nos alertas de posts do X")
        await asyncio.sleep(X_POSTS_POLL_SECONDS)


def _log_block(c: HCand, why: str):
    """Registra (uma vez por motivo) por que um token com bom potencial não virou alerta."""
    if c.last_block != why:
        c.last_block = why
        log.info("Sem alerta em %s ($%s): %s", c.name or c.mint[:8], c.symbol, why)

def _alert_allowed() -> bool:
    now = time.time()
    while alert_times and now - alert_times[0] > 3600:
        alert_times.popleft()
    return len(alert_times) < MAX_ALERTS_PER_HOUR

async def evaluate_once(bot):
    now = time.time()
    for mint, c in list(pool.items()):
        if c.alerted or now - c.created_at > MAX_AGE_MIN * 60:
            pool.pop(mint, None)
        elif c.source == "x" and not c.pair_seen and now - c.created_at > X_WAIT_NO_PAIR_MIN * 60:
            pool.pop(mint, None)   # endereço que não virou token no DexScreener
            _x_skip.add(mint)

    ready = [m for m, c in pool.items() if c.meta_done or now - c.created_at > 45]
    if not ready:
        return
    pairs = await fetch_pairs(ready)

    for mint in ready:
        c = pool.get(mint)
        if not c:
            continue
        pair = pairs.get(mint)
        mk = market_numbers(pair)
        if mk and mk["mcap"] > 0 and c.first_mcap is None:
            c.first_mcap = mk["mcap"]

        if c.source == "x" and pair:
            c.pair_seen = True
            created_ms = fnum(pair.get("pairCreatedAt"))
            if created_ms > 0:
                c.token_age_min = max(0.0, (now * 1000 - created_ms) / 60000)
                if c.token_age_min > X_MAX_TOKEN_AGE_MIN:   # token velho: não é lançamento novo
                    pool.pop(mint, None)
                    _x_skip.add(mint)
                    continue
            if not c.name:
                bt = pair.get("baseToken") or {}
                c.name, c.symbol = str(bt.get("name") or ""), str(bt.get("symbol") or "")
            if not c.tags_named:
                c.tags = sorted(set(c.tags) | set(detect_tags(f"{c.name} {c.symbol}")))
                c.tags_named = True

        base, _, _, _ = score_candidate(c, mk, c.xi)
        c.last_score = base

        min_buys = X_MIN_BUYS_5M if c.source == "x" else MIN_BUYS_5M
        traction_ok = ALERT_WITHOUT_TRACTION or (mk is not None and mk["buys"] >= min_buys)
        mcap_ok = mk is None or mk["mcap"] <= 0 or mk["mcap"] <= MAX_MCAP_USD
        if base >= MIN_SCORE - 12 and not (traction_ok and mcap_ok):
            why = []
            if not traction_ok:
                why.append(f"tração baixa ({mk['buys'] if mk else 0} compras em 5 min, mínimo {min_buys})")
            if not mcap_ok:
                why.append(f"market cap {usd(mk['mcap'])} acima do limite de {usd(MAX_MCAP_USD)}")
            _log_block(c, " e ".join(why))
        if not (traction_ok and mcap_ok and base >= MIN_SCORE - 12 and now - c.deep_at > 300):
            continue
        if not _alert_allowed():
            log.info("Limite de %s alertas/hora atingido; %s fica para a próxima rodada.", MAX_ALERTS_PER_HOUR, c.symbol)
            continue

        # Checagens mais caras (RPC e X) só para quem já é candidato forte
        c.deep_at = now
        sec = None
        try:
            sec = await check_token_security(mint)
        except Exception as e:
            log.warning("Falha na checagem de segurança de %s: %s", c.symbol, e)
        if sec is not None and not (sec.mint_revoked and sec.freeze_revoked):
            log.info("%s descartado: mint/freeze authority ativa.", c.symbol)
            c.alerted = True  # não reavalia
            continue
        xi = c.xi if c.source == "x" else None   # tokens vindos do X já trazem os dados do perfil (sem custo extra)
        if c.source != "x":
            try:
                xi = await x_enrich(c)
            except Exception as e:
                log.warning("Falha ao consultar o X para %s: %s", c.symbol, e)

        score, reasons, risks, parts = score_candidate(c, mk, xi, sec)
        c.last_score = score
        if score < MIN_SCORE:
            _log_block(c, f"score {score} abaixo do mínimo de {MIN_SCORE}")
            continue

        await notify(bot, build_alert(c, score, reasons, risks, mk, parts), public=True, source=c.source)
        c.alerted = True
        alert_times.append(now)
        hunter_seen.append(mint)
        save_seen()
        mc = mk["mcap"] if mk and mk["mcap"] > 0 else None
        alert_records.append({
            "mint": mint, "name": c.name, "symbol": c.symbol, "alerted_at": now, "score": score,
            "tags": c.tags, "has_x": bool(c.x_url), "has_site": bool(c.website), "source": c.source,
            "entry_mcap": mc, "peak_mcap": mc, "last_mcap": mc,
            "milestones": [], "warned_drop": False, "warned_stop": False,
        })
        save_alerts()
        log.info("🏹 ALERTA: %s ($%s) score=%s mcap=%s", c.name, c.symbol, score, usd(mc))

async def evaluator_loop(bot):
    while True:
        await asyncio.sleep(CHECK_SECONDS)
        try:
            await evaluate_once(bot)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Erro na avaliação dos lançamentos")


# ==========================================
# Acompanhamento dos alertas (2x, 3x, 5x, 10x, recuo)
# ==========================================
async def track_once(bot):
    now = time.time()
    active = [r for r in alert_records if r.get("entry_mcap") and now - r["alerted_at"] <= TRACK_HOURS * 3600]
    if not active:
        return
    pairs = await fetch_pairs([r["mint"] for r in active])
    changed = False
    for r in active:
        mk = market_numbers(pairs.get(r["mint"]))
        if not mk or mk["mcap"] <= 0:
            continue
        m = mk["mcap"]
        r["last_mcap"] = m
        r["peak_mcap"] = max(r.get("peak_mcap") or m, m)
        changed = True
        if not MILESTONES:
            continue
        mult = m / r["entry_mcap"]
        peak_mult = r["peak_mcap"] / r["entry_mcap"]
        sym = esc(r["symbol"])
        for level in (2, 3, 5, 10):
            if peak_mult >= level and level not in r["milestones"]:
                r["milestones"].append(level)
                await notify(bot,
                    f"🚀 <b>${sym} chegou a {level}x</b> desde o alerta (agora {usd(m)}, entrada {usd(r['entry_mcap'])}).\n"
                    f"Realizar parte do lucro é uma estratégia comum; a decisão é sua.\n"
                    f"<code>{r['mint']}</code>", public=True, source=r.get("source", ""))
        if peak_mult >= 1.5 and m <= r["peak_mcap"] * 0.7 and not r["warned_drop"]:
            r["warned_drop"] = True
            await notify(bot,
                f"⚠️ <b>${sym} recuou 30% do topo</b> (topo {usd(r['peak_mcap'])} → agora {usd(m)}; {mult:.2f}x do alerta).\n"
                f"<code>{r['mint']}</code>", public=True, source=r.get("source", ""))
        elif mult <= 0.7 and not r["warned_stop"]:
            r["warned_stop"] = True
            await notify(bot,
                f"🔻 <b>${sym} caiu 30% desde o alerta</b> (agora {usd(m)}; entrada {usd(r['entry_mcap'])}).\n"
                f"<code>{r['mint']}</code>", public=True, source=r.get("source", ""))
    if changed:
        save_alerts()

async def tracker_loop(bot):
    while True:
        await asyncio.sleep(60)
        try:
            await track_once(bot)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Erro no acompanhamento dos alertas")


# ==========================================
# Desempenho real dos alertas
# ==========================================
def build_report() -> str:
    recs = [r for r in alert_records if r.get("entry_mcap") and r.get("peak_mcap") and r.get("last_mcap")]
    if not recs:
        return "📊 <b>Desempenho do Caçador</b>\n\nAinda não há alertas acompanhados com market cap registrado."

    def stats(rs: list[dict]) -> str:
        peaks = [r["peak_mcap"] / r["entry_mcap"] for r in rs]
        down = sum(1 for r in rs if r["last_mcap"] <= 0.5 * r["entry_mcap"])
        return (
            f"{len(rs)} alertas | ≥2x: {sum(p >= 2 for p in peaks)} ({100 * sum(p >= 2 for p in peaks) / len(rs):.0f}%) | "
            f"≥5x: {sum(p >= 5 for p in peaks)} | pico mediano: {statistics.median(peaks):.2f}x | "
            f"hoje ≤ metade da entrada: {down} ({100 * down / len(rs):.0f}%)"
        )

    full = [r for r in recs if r.get("has_x") and r.get("has_site")]
    rest = [r for r in recs if not (r.get("has_x") and r.get("has_site"))]
    msg = ["📊 <b>Desempenho do Caçador</b> (market cap no alerta × pico e valor atual)", "", f"<b>Todos:</b> {stats(recs)}"]
    if full:
        msg.append(f"<b>Com X + site:</b> {stats(full)}")
    if rest:
        msg.append(f"<b>Sem X ou sem site:</b> {stats(rest)}")
    via_x = [r for r in recs if r.get("source") == "x"]
    if via_x:
        msg.append(f"<b>Descobertos pelo X (CA postado):</b> {stats(via_x)}")
    msg += [
        "",
        "<i>O pico é medido a cada ~60s, então pode estar subestimado. Não considera taxas, slippage nem se você "
        "conseguiria comprar/vender nesses preços. Resultado passado não garante o futuro.</i>",
    ]
    return "\n".join(msg)

async def report_loop(bot):
    if REPORT_HOURS <= 0:
        return
    while True:
        await asyncio.sleep(REPORT_HOURS * 3600)
        try:
            await notify(bot, build_report())
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Erro ao enviar o resumo de desempenho")


# ==========================================
# Comandos do Telegram (só com HUNTER_TELEGRAM_BOT_TOKEN próprio)
# ==========================================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🏹 Caçador de moedas novas ativo.\n/status — situação atual\n/desempenho — como os alertas se saíram\n/ultimos — últimos alertas"
    )

async def cmd_chatid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"ID deste chat: <code>{update.effective_chat.id}</code>", parse_mode="HTML")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🏹 <b>Caçador</b>\n"
        f"Tokens em observação: {len(pool)}\n"
        f"Alertas enviados: {len(alert_records)}\n"
        f"Regras: potencial ≥ {MIN_SCORE} | idade ≤ {MAX_AGE_MIN:g} min | market cap ≤ {usd(MAX_MCAP_USD)} | ≥ {MIN_BUYS_5M} compras/5 min\n"
        f"X API: {'ativa' if ENABLE_X_SEARCH and X_BEARER_TOKEN else 'desligada (sem checagem de conta/CA)'}\n"
        f"Alertas ao público: {(str(len(load_public_subscribers())) + ' inscrito(s) do radar') if BROADCAST and TELEGRAM_BOT_TOKEN else 'desligados'}",
        parse_mode="HTML",
    )

async def cmd_desempenho(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(build_report(), parse_mode="HTML")

async def cmd_ultimos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    recs = alert_records[-6:][::-1]
    if not recs:
        await update.message.reply_text("Nenhum alerta ainda.")
        return
    lines = ["🏹 <b>Últimos alertas</b>"]
    for r in recs:
        em, lm = r.get("entry_mcap"), r.get("last_mcap")
        now_x = f"{lm / em:.2f}x" if em and lm else "N/D"
        pk = f"{r['peak_mcap'] / em:.2f}x" if em and r.get("peak_mcap") else "N/D"
        lines.append(f"• <b>${esc(r['symbol'])}</b> score {r['score']} | agora {now_x} | pico {pk}\n  <code>{r['mint']}</code>")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


# ==========================================
# Inicialização
# ==========================================
def startup_text() -> str:
    return (
        "🏹 <b>Caçador de moedas novas conectado</b>\n"
        f"Regras: potencial ≥ {MIN_SCORE} | idade ≤ {MAX_AGE_MIN:g} min | market cap ≤ {usd(MAX_MCAP_USD)} | ≥ {MIN_BUYS_5M} compras/5 min\n"
        f"X API: {'ativa' if ENABLE_X_SEARCH and X_BEARER_TOKEN else 'desligada'} | "
        f"Comandos: {'ativos' if COMMANDS_ENABLED else 'desligados (sem HUNTER_TELEGRAM_BOT_TOKEN)'}\n"
        f"Alertas ao público: {(str(len(load_public_subscribers())) + ' inscrito(s) do radar') if BROADCAST and TELEGRAM_BOT_TOKEN else 'desligados'}\n"
        + (f"Alertas de CA postado no X vão para o chat {X_CHAT_ID}{' (e também para o principal)' if X_ALSO_MAIN else ''}\n" if X_CHAT_ID else "")
        + f"Descoberta pelo X (perfis que postam o CA): {('ativa, até ' + str(X_DAILY_BUDGET) + ' posts lidos/dia') if x_discovery_active() else 'desligada (precisa de ENABLE_X_SEARCH=true e X_BEARER_TOKEN)'}\n"
        + ("" if x_discovery_active() else f"🔎 {x_diagnostic()}\n")
        + (f"Alertas de posts do X: {', '.join('@' + a for a in X_POST_ACCOUNTS)}\n" if X_POST_ACCOUNTS and x_api_ready() else "")
        + (f"Perfis do X vigiados: {', '.join('@' + h for h in X_WATCH)}\n" if X_WATCH else "")
        + "Não compro nem vendo nada: as decisões são suas. A maioria dos tokens novos perde quase todo o valor."
    )

async def start_background(bot) -> list:
    global meta_queue, session, public_bot
    session = aiohttp.ClientSession(headers={"User-Agent": "MemecoinHunter/1.0"})
    meta_queue = asyncio.Queue(maxsize=5000)
    load_state()
    load_x_state()
    if BROADCAST and TELEGRAM_BOT_TOKEN:
        if BOT_TOKEN == TELEGRAM_BOT_TOKEN:
            public_bot = bot                               # mesmo bot: reaproveita
        else:
            public_bot = Bot(TELEGRAM_BOT_TOKEN)           # inscritos só recebem do bot do radar
            await public_bot.initialize()
    elif BROADCAST:
        log.warning("TELEGRAM_BOT_TOKEN (do radar) ausente: alertas ao público desligados.")
    await notify(bot, startup_text())
    coros = [listener(), x_poll_loop(bot), x_posts_loop(bot), evaluator_loop(bot), tracker_loop(bot), report_loop(bot)] + [meta_worker() for _ in range(META_WORKERS)]
    return [asyncio.create_task(c) for c in coros]

async def post_init(app: Application):
    app.bot_data["tasks"] = await start_background(app.bot)

async def close_public_bot(current_bot):
    if public_bot is not None and public_bot is not current_bot:
        try:
            await public_bot.shutdown()
        except Exception:
            pass

async def post_shutdown(app: Application):
    for t in app.bot_data.get("tasks", []):
        t.cancel()
    await close_public_bot(app.bot)
    if session:
        await session.close()

async def run_send_only():
    async with Bot(BOT_TOKEN) as bot:
        tasks = await start_background(bot)
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            await close_public_bot(bot)
            if session:
                await session.close()

async def explain_cli(mint: str):
    """Mostra, no terminal, como o Caçador avaliaria este token AGORA e qual trava o bloquearia."""
    global session
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    mint = mint.strip()
    if not re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", mint):
        print("Endereço inválido. Use: py bot_cacador.py --explicar <CA completo>")
        return
    session = aiohttp.ClientSession(headers={"User-Agent": "MemecoinHunter/1.0"})
    try:
        pair = (await fetch_pairs([mint])).get(mint)
        if not pair:
            print("O DexScreener não tem esse token (ainda). O Caçador só avalia tokens que aparecem lá.")
            return
        bt = pair.get("baseToken") or {}
        c = HCand(mint=mint, name=str(bt.get("name") or ""), symbol=str(bt.get("symbol") or ""), uri="", dev="",
                  initial_buy=None, created_at=time.time(), meta_done=True)
        info = pair.get("info") or {}
        for so in info.get("socials") or []:
            url = str(so.get("url") or "")
            if so.get("type") == "twitter":
                px = parse_x_link(url)
                if px:
                    c.x_url, c.x_user, c.x_kind = url, px["user"], px["kind"]
            elif so.get("type") == "telegram":
                c.telegram = url
        for w in info.get("websites") or []:
            c.website = c.website or valid_website(str(w.get("url") or ""))
        c.tags = sorted(set(detect_tags(f"{c.name} {c.symbol}")) | ({"post_x"} if c.x_kind == "status" else set()))
        mk = market_numbers(pair)
        created_ms = fnum(pair.get("pairCreatedAt"))
        age_min = max(0.0, (time.time() * 1000 - created_ms) / 60000) if created_ms > 0 else None
        sec = None
        try:
            sec = await check_token_security(mint)
        except Exception as e:
            print(f"(checagem de segurança indisponível: {e})")
        score, reasons, risks, parts = score_candidate(c, mk, None, sec)

        ok = lambda b: "✅" if b else "❌"
        print(f"\n{c.name} (${c.symbol})  CA: {mint}")
        print(f"Idade do par: {f'{age_min:.0f} min' if age_min is not None else 'N/D'} | Market cap: {usd(mk['mcap']) if mk else 'N/D'} | "
              f"5 min: {mk['buys'] if mk else 0} compras / {mk['sells'] if mk else 0} vendas")
        print(f"Social: X {'sim' if c.x_url else 'não'} | site {'sim' if c.website else 'não'} | Telegram {'sim' if c.telegram else 'não'}"
              "  (lido do DexScreener; a descrição do lançamento não está disponível aqui)")
        print(f"Narrativa detectada: {', '.join(TAG_LABELS.get(t, t) for t in c.tags) or 'nenhuma'}")
        print(f"\nScore de potencial: {score}/100  (social {parts['social']}/35, narrativa {parts['narrativa']}/35, "
              f"tração {parts['tracao']}/25, riscos -{parts['penalidades']})")
        gates = [
            (f"Idade do par ≤ {MAX_AGE_MIN:g} min (o Caçador só acompanha lançamentos vistos nesse prazo)", age_min is not None and age_min <= MAX_AGE_MIN),
            (f"Market cap ≤ {usd(MAX_MCAP_USD)}", bool(mk) and (mk['mcap'] <= 0 or mk['mcap'] <= MAX_MCAP_USD)),
            (f"Pelo menos {MIN_BUYS_5M} compras em 5 min", bool(mk) and mk['buys'] >= MIN_BUYS_5M),
            (f"Score ≥ {MIN_SCORE}", score >= MIN_SCORE),
            ("Mint e freeze authority revogadas", sec is None or (sec.mint_revoked and sec.freeze_revoked)),
        ]
        print("\nTravas:")
        for name, passed in gates:
            print(f"  {ok(passed)} {name}")
        blocked = [n for n, passed in gates if not passed]
        print("\n" + ("➡️  Com os números de AGORA, o Caçador alertaria." if not blocked else "➡️  Com os números de AGORA, seria bloqueado por: " + "; ".join(blocked)))
        print("Obs.: os números mudam rápido. No momento do lançamento, market cap e compras eram outros.")
        if reasons:
            print("\nPontos a favor: " + "; ".join(reasons[:6]))
        if risks:
            print("Riscos: " + "; ".join(risks[:6]))
    finally:
        await session.close()

def main():
    if len(sys.argv) >= 2 and sys.argv[1] in ("--explicar", "--explain"):
        if len(sys.argv) < 3:
            raise SystemExit("Use: py bot_cacador.py --explicar <CA do token>")
        asyncio.run(explain_cli(sys.argv[2]))
        return
    if not BOT_TOKEN:
        raise SystemExit("Configure HUNTER_TELEGRAM_BOT_TOKEN (ou TELEGRAM_BOT_TOKEN) no arquivo .env")
    if not CHAT_ID:
        raise SystemExit("Configure HUNTER_TELEGRAM_CHAT_ID (ou TELEGRAM_CHAT_ID) no arquivo .env")

    if COMMANDS_ENABLED:
        app = Application.builder().token(BOT_TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()
        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CommandHandler("status", cmd_status))
        app.add_handler(CommandHandler("desempenho", cmd_desempenho))
        app.add_handler(CommandHandler("ultimos", cmd_ultimos))
        app.add_handler(CommandHandler("chatid", cmd_chatid))
        log.info("Caçador iniciado com comandos (bot próprio).")
        app.run_polling(allowed_updates=Update.ALL_TYPES)
    else:
        log.info("Caçador iniciado só com alertas (usando o token do radar apenas para enviar mensagens).")
        try:
            asyncio.run(run_send_only())
        except KeyboardInterrupt:
            pass

if __name__ == "__main__":
    main()
