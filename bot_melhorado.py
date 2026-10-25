"""
Solana Memecoin Radar — Bot de detecção de novas memecoins
Arquivo Base: bot_melhorado.py (Versão com Smart Money Tracker e Metodologia dos Calloors)

Recursos Principais:
1. Smart Money Tracker em Tempo Real (Helius Enhanced Transactions):
   - Monitoramento contínuo das carteiras dos maiores calloors/traders da Solana (ex: buddhaaaaa, rikz_, thedetective).
   - Alerta imediato no Telegram quando qualquer carteira monitorada comprar um novo token.
   - Comandos /addwallet, /delwallet, /wallets para gerenciar a lista diretamente no Telegram.
2. Metodologia Alpha dos Calloors no Motor Heurístico:
   - Sweet Spot de Market Cap ($3k a $10k): valorização de tokens no estágio exato em que os grandes calloors entram.
   - Bônus especial de pontuação (+30 pts) se uma carteira de Smart Money confirmou a compra.
3. Descoberta em Tempo Real: Ingestão de novos tokens via PumpPortal WebSocket (Pump.fun) e DexScreener.
4. Suporte Completo a Pump.fun: Cálculo inteligente de liquidez efetiva para bonding curves.
5. Ciclo de Vida de Candidatos: Máquina de estados (DETECTED -> OBSERVING -> ALERT_ELIGIBLE -> ALERTED -> EXPIRED).
6. Auditoria On-Chain Anti-Rugpull (Helius & Solana RPC + Rugcheck):
   - Checagem de Mint Authority e Freeze Authority.
   - Concentração de carteira individual (alerta se dev/whale detém >= 20-25%).
   - Detecção precisa de Multi-Carteiras / Sybil Bundling do dev.
   - Top 10 concentração de holders (excluindo pools).
7. Comandos Interativos: /start, /status, /radar, /check <mint>, /wallets, /addwallet, /delwallet.
"""

import os
import re
import sys
import time
import functools
import faulthandler
import json
import logging
import asyncio
from typing import Any, Optional
from dataclasses import dataclass, field
import aiohttp
import websockets
from dotenv import load_dotenv
import telegram
from telegram import Update
from telegram.error import BadRequest, Forbidden, RetryAfter
from telegram.ext import Application, CommandHandler, ContextTypes

# ==========================================
# Configurações Iniciais e Variáveis de Ambiente
# ==========================================
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
for _noisy in ("httpx", "httpcore"):   # estes logs imprimem a URL do Telegram com o token do bot
    logging.getLogger(_noisy).setLevel(logging.WARNING)
faulthandler.enable()  # se o Python falhar de forma nativa, imprime o motivo em vez de fechar em silêncio
log = logging.getLogger("memecoin-radar")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

POLL_SECONDS = max(10, int(os.getenv("POLL_SECONDS", "30")))
# Intervalo de consulta das carteiras smart (cada consulta custa 1 crédito Helius por carteira)
SMART_POLL_SECONDS = max(10, int(os.getenv("SMART_POLL_SECONDS", "20")))
# Transações das carteiras smart mais antigas que isso (minutos) são ignoradas (evita alertas atrasados após reiniciar)
SMART_MAX_TX_AGE_MIN = max(1, int(os.getenv("SMART_MAX_TX_AGE_MIN", "20")))
MIN_SCORE = max(0, min(100, int(os.getenv("MIN_SCORE", "55"))))
MIN_LIQUIDITY_USD = max(0, float(os.getenv("MIN_LIQUIDITY_USD", "2000")))
MAX_TOKEN_AGE_HOURS = max(0.1, float(os.getenv("MAX_TOKEN_AGE_HOURS", "3.0")))

# Integração Helius & Solana RPC
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "").strip()
if HELIUS_API_KEY:
    SOLANA_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    log.info("Helius RPC ativado com API Key informada.")
else:
    SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com").strip()
    log.info("Usando RPC Solana padrão: %s", SOLANA_RPC_URL)

# Descoberta em tempo real
ENABLE_PUMPPORTAL = os.getenv("ENABLE_PUMPPORTAL", "true").lower() == "true"
PUMPPORTAL_WS_URL = "wss://pumpportal.fun/api/data"

# Parâmetros Anti-Rugpull & Concentração de Holders
MAX_SINGLE_HOLDER_PCT = float(os.getenv("MAX_SINGLE_HOLDER_PCT", "20.0"))
MAX_TOP10_HOLDERS_PCT = float(os.getenv("MAX_TOP10_HOLDERS_PCT", "45.0"))
ENABLE_BUNDLED_DETECTION = os.getenv("ENABLE_BUNDLED_DETECTION", "true").lower() == "true"

# Redes Sociais & Arquivos de Estado
ENABLE_X_SEARCH = os.getenv("ENABLE_X_SEARCH", "false").lower() == "true"
# Regras de COMPRA (o alerta continua sendo enviado a partir de MIN_SCORE)
BUY_MIN_SCORE = max(0, min(100, int(os.getenv("BUY_MIN_SCORE", "70"))))
REQUIRE_CA_ON_X = os.getenv("REQUIRE_CA_ON_X", "true").strip().lower() in ("1", "true", "yes", "sim")
X_CA_MIN_MENTIONS = max(1, int(os.getenv("X_CA_MIN_MENTIONS", "1")))
X_CACHE_SECONDS = max(60, int(os.getenv("X_CACHE_SECONDS", "900")))
# Alertas para todos: quem abrir o bot e enviar /start passa a receber os alertas (/stop cancela)
BROADCAST_ALERTS = os.getenv("BROADCAST_ALERTS", "true").strip().lower() in ("1", "true", "yes", "sim")
MAX_SUBSCRIBERS = max(1, int(os.getenv("MAX_SUBSCRIBERS", "1000")))
SUBSCRIBERS_FILE = os.getenv("SUBSCRIBERS_FILE", "subscribers.json")
CHECK_COOLDOWN_SECONDS = max(0, int(os.getenv("CHECK_COOLDOWN_SECONDS", "30")))  # intervalo do /check para não-admins
X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN", "").strip()
STATE_FILE = os.getenv("STATE_FILE", "alerted_tokens.json")
SMART_WALLETS_FILE = os.getenv("SMART_WALLETS_FILE", "smart_wallets.json")
SIMULATION_FILE = os.getenv("SIMULATION_FILE", "simulation_state.json")

# Configurações de Simulação de Compras (Paper Trading)
ENABLE_SIMULATION = os.getenv("ENABLE_SIMULATION", "true").lower() == "true"
SIM_INITIAL_CAPITAL = float(os.getenv("SIM_INITIAL_CAPITAL", "20.0"))
SIM_BUY_AMOUNT = float(os.getenv("SIM_BUY_AMOUNT", "2.0"))
SIM_STOP_LOSS_PCT = float(os.getenv("SIM_STOP_LOSS_PCT", "20.0"))
SIM_TRAILING_STOP_PCT = float(os.getenv("SIM_TRAILING_STOP_PCT", "20.0"))
# Reforços (comprar mais após +25%) desligados por padrão: aumentavam a perda nos rugs
SIM_ENABLE_ADDONS = os.getenv("SIM_ENABLE_ADDONS", "false").strip().lower() in ("1", "true", "yes", "sim")
# Máximo de posições abertas ao mesmo tempo
SIM_MAX_OPEN_POSITIONS = max(1, int(os.getenv("SIM_MAX_OPEN_POSITIONS", "3")))

DEX_BASE = "https://api.dexscreener.com"
RUGCHECK_BASE = "https://api.rugcheck.xyz/v1"

# Endereços e programas conhecidos
PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
RAYDIUM_AUTHORITY = "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1"
BURN_ADDRESSES = {
    "11111111111111111111111111111111",
    "1nc1nerator11111111111111111111111111111111",
    "dead111111111111111111111111111111111111111",
}

# ==========================================
# Estruturas de Dados
# ==========================================
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

@dataclass
class CandidateToken:
    mint: str
    source: str
    detected_at: float
    name: str = ""
    symbol: str = ""
    check_count: int = 0
    last_checked_at: float = 0.0
    status: str = "DETECTED"  # DETECTED, OBSERVING, ALERT_ELIGIBLE, ALERTED, EXPIRED, DISCARDED
    initial_buy_sol: Optional[float] = None
    last_score: int = 0
    last_liquidity: float = 0.0
    last_volume: float = 0.0
    smart_buyer: Optional[str] = None

# ==========================================
# Estado Global & Carteiras de Smart Money
# ==========================================
seen: set[str] = set()
subscribers: dict[str, dict] = {}          # chat_id -> {"since": timestamp}
_bg_tasks: set = set()                  # referências das tarefas de envio em segundo plano
_check_last: dict[int, float] = {}       # último /check por chat (limite de uso)
MINT_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
smart_status: dict[str, dict] = {}   # saúde da consulta de cada carteira smart
smart_seen: set[str] = set()         # "carteira:token" já alertados (alerta por carteira, independente do radar)
last_rpc_error: str = ""             # último erro de RPC (para diagnóstico)
buy_watch: set[str] = set()  # alertados que ainda podem ser comprados (ex: CA ainda não apareceu no X)
candidate_pool: dict[str, CandidateToken] = {}
smart_wallets: dict[str, dict] = {}  # {address: {"name": str, "last_tx": str}}
session: aiohttp.ClientSession | None = None

# ==========================================
# Persistência de Tokens e Carteiras
# ==========================================
def load_seen():
    global seen
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            seen = set(data if isinstance(data, list) else [])
    except (FileNotFoundError, json.JSONDecodeError):
        seen = set()

def save_seen():
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(seen)[-10000:], f, indent=2)
    except OSError as e:
        log.warning("Não foi possível salvar estado: %s", e)

def load_smart_wallets():
    global smart_wallets
    try:
        with open(SMART_WALLETS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            smart_wallets = data.get("wallets", {})
            log.info("Carregadas %s carteiras de Smart Money para monitoramento.", len(smart_wallets))
    except (FileNotFoundError, json.JSONDecodeError):
        smart_wallets = {}

def save_smart_wallets():
    try:
        with open(SMART_WALLETS_FILE, "w", encoding="utf-8") as f:
            json.dump({"wallets": smart_wallets}, f, indent=2)
    except OSError as e:
        log.warning("Não foi possível salvar smart wallets: %s", e)

# ==========================================
# Motor de Simulação de Compras (Paper Trading)
# ==========================================
simulation_portfolio: dict = {
    "initial_capital": SIM_INITIAL_CAPITAL,
    "cash_usd": SIM_INITIAL_CAPITAL,
    "positions": {},
    "closed_trades": []
}

def load_simulation():
    global simulation_portfolio
    try:
        with open(SIMULATION_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            simulation_portfolio = {
                "initial_capital": float(data.get("initial_capital", SIM_INITIAL_CAPITAL)),
                "cash_usd": float(data.get("cash_usd", SIM_INITIAL_CAPITAL)),
                "positions": data.get("positions", {}),
                "closed_trades": data.get("closed_trades", [])
            }
            log.info("Simulação carregada: Saldo em caixa = $%.2f USD | Posições abertas = %s", simulation_portfolio["cash_usd"], len(simulation_portfolio["positions"]))
    except (FileNotFoundError, json.JSONDecodeError):
        simulation_portfolio = {
            "initial_capital": SIM_INITIAL_CAPITAL,
            "cash_usd": SIM_INITIAL_CAPITAL,
            "positions": {},
            "closed_trades": []
        }

def save_simulation():
    try:
        with open(SIMULATION_FILE, "w", encoding="utf-8") as f:
            json.dump(simulation_portfolio, f, indent=2)
    except OSError as e:
        log.warning("Não foi possível salvar estado de simulação: %s", e)

def get_token_price_from_pair(pair: dict) -> float:
    price_usd = pair.get("priceUsd")
    if price_usd:
        try:
            p = float(price_usd)
            if p > 0:
                return p
        except (ValueError, TypeError):
            pass
    fdv = float(pair.get("fdv") or pair.get("marketCap") or 0)
    if fdv > 0:
        return fdv / 1_000_000_000
    return 0.0

async def sim_open_position(mint: str, name: str, symbol: str, price: float, reason: str, app: Application):
    """Abre uma nova posição simulada com compras de $2.00."""
    if not ENABLE_SIMULATION or price <= 0:
        return

    if mint in simulation_portfolio.get("positions", {}):
        return

    if len(simulation_portfolio.get("positions", {})) >= SIM_MAX_OPEN_POSITIONS:
        log.info("Simulação: limite de %s posições abertas atingido; ignorando %s", SIM_MAX_OPEN_POSITIONS, symbol)
        return

    cash = simulation_portfolio.get("cash_usd", 0.0)
    if cash < SIM_BUY_AMOUNT:
        log.info("Simulação: saldo em caixa insuficiente ($%.2f < $%.2f) para abrir posição em %s", cash, SIM_BUY_AMOUNT, symbol)
        return

    tokens = SIM_BUY_AMOUNT / price
    hard_stop = price * (1.0 - SIM_STOP_LOSS_PCT / 100.0)
    take_profit = price * (1.0 + SIM_STOP_LOSS_PCT / 100.0)

    simulation_portfolio["cash_usd"] -= SIM_BUY_AMOUNT
    simulation_portfolio["positions"][mint] = {
        "name": name,
        "symbol": symbol,
        "entry_price": price,
        "peak_price": price,
        "current_price": price,
        "amount_tokens": tokens,
        "invested_usd": SIM_BUY_AMOUNT,
        "hard_stop": hard_stop,
        "trailing_stop": hard_stop,
        "trailing_active": False,
        "opened_at": time.time(),
        "reason": reason,
        "addons": 0,
        "last_addon_at": 0.0
    }
    save_simulation()
    log.info("🛒 SIMULAÇÃO COMPRA: %s ($%s) a $%.8f (Investido: $%.2f | Caixa restante: $%.2f)", name, symbol, price, SIM_BUY_AMOUNT, simulation_portfolio["cash_usd"])

    if TELEGRAM_CHAT_ID:
        try:
            msg = (
                f"🛒 <b>SIMULAÇÃO: COMPRA EXECUTADA</b>\n\n"
                f"🪙 <b>Token:</b> {name} (${symbol})\n"
                f"💵 <b>Investimento:</b> ${SIM_BUY_AMOUNT:.2f} ({tokens:,.0f} tokens)\n"
                f"🏷️ <b>Preço de Entrada:</b> ${price:.8f}\n"
                f"🛑 <b>Stop Loss (-{SIM_STOP_LOSS_PCT:g}%):</b> ${hard_stop:.8f}\n"
                f"🎯 <b>Alvo Inicial (+{SIM_STOP_LOSS_PCT:g}%):</b> ${take_profit:.8f} (Ativa Trailing Stop)\n"
                f"💡 <b>Gatilho:</b> {reason}\n\n"
                f"💰 <b>Saldo em Caixa Restante:</b> ${simulation_portfolio['cash_usd']:.2f} USD\n"
                f"🪙 <b>Contrato:</b> <code>{mint}</code>"
            )
            await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg, parse_mode="HTML")
        except Exception as e:
            log.warning("Falha ao enviar mensagem de compra simulada: %s", e)

async def sim_addon_position(mint: str, current_price: float, app: Application):
    if not SIM_ENABLE_ADDONS:
        return
    """Piramidagem: Realiza pequenas compras adicionais na alta para surfar a tendência."""
    if not ENABLE_SIMULATION or mint not in simulation_portfolio.get("positions", {}):
        return

    pos = simulation_portfolio["positions"][mint]
    if current_price < pos["entry_price"] * 1.25:
        return
    if pos.get("addons", 0) >= 2:
        return
    if time.time() - pos.get("last_addon_at", 0) < 180:
        return

    cash = simulation_portfolio.get("cash_usd", 0.0)
    if cash < 1.0:
        return

    addon_usd = min(1.50, cash)
    additional_tokens = addon_usd / current_price

    pos["amount_tokens"] += additional_tokens
    pos["invested_usd"] += addon_usd
    pos["entry_price"] = pos["invested_usd"] / pos["amount_tokens"]
    pos["addons"] = pos.get("addons", 0) + 1
    pos["last_addon_at"] = time.time()
    simulation_portfolio["cash_usd"] -= addon_usd
    save_simulation()

    gain_pct = ((current_price - pos["entry_price"]) / pos["entry_price"]) * 100.0
    log.info("📈 SIMULAÇÃO REFORÇO: %s mais $%.2f (Novo PM: $%.8f)", pos['symbol'], addon_usd, pos['entry_price'])

    if TELEGRAM_CHAT_ID:
        try:
            msg = (
                f"📈 <b>SIMULAÇÃO: REFORÇO DE POSIÇÃO (DCA NA ALTA)</b>\n\n"
                f"🪙 <b>Token:</b> {pos['name']} (${pos['symbol']}) em forte alta (+{gain_pct:.1f}%)!\n"
                f"💵 <b>Aporte Adicional:</b> ${addon_usd:.2f}\n"
                f"🏷️ <b>Novo Preço Médio:</b> ${pos['entry_price']:.8f}\n"
                f"🛑 <b>Trailing Stop Atual:</b> ${pos['trailing_stop']:.8f}\n"
                f"💰 <b>Saldo Restante em Caixa:</b> ${simulation_portfolio['cash_usd']:.2f} USD"
            )
            await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg, parse_mode="HTML")
        except Exception as e:
            log.warning("Falha ao enviar mensagem de reforço: %s", e)

def fmt_pnl(v: float) -> str:
    """Formata resultado com o sinal antes do $ (ex: +$1.20 / -$4.98)."""
    return f"{'+' if v >= 0 else '-'}${abs(v):.2f}"

async def confirm_exit_price(mint: str, pos: dict, price: float, prev_price: float, stop_price: float):
    """
    Protege contra leitura de preço ruim: se o preço despencou >50% desde a leitura anterior,
    relê o DexScreener alguns segundos depois antes de vender.
    Retorna o preço a usar na venda, ou None para adiar a venda.
    """
    if prev_price <= 0 or price >= prev_price * 0.5:
        return price
    await asyncio.sleep(4)
    data = await get_json(f"{DEX_BASE}/latest/dex/tokens/{mint}")
    pairs = [p for p in (data.get("pairs") or []) if p.get("chainId") == "solana"] if isinstance(data, dict) else []
    fails = pos.get("exit_confirm_fails", 0)
    if not pairs:
        pos["exit_confirm_fails"] = fails + 1
        log.warning("Queda brusca em %s sem par no DexScreener (tentativa %s/3)", pos.get("symbol"), fails + 1)
        return price if fails + 1 >= 3 else None
    pair = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0) + float((p.get("volume") or {}).get("h1") or 0))
    p2 = get_token_price_from_pair(pair)
    liq = float((pair.get("liquidity") or {}).get("usd") or 0)
    log.warning("Queda brusca em %s: %.3g -> %.3g -> %.3g (liq $%.0f, dex %s)",
                pos.get("symbol"), prev_price, price, p2, liq, pair.get("dexId"))
    if p2 <= 0:
        pos["exit_confirm_fails"] = fails + 1
        return price if fails + 1 >= 3 else None
    if p2 > stop_price:
        pos["exit_confirm_fails"] = 0
        return None  # leitura anterior foi um falso alarme
    pos["exit_confirm_fails"] = 0
    return p2

async def sim_close_position(mint: str, current_price: float, close_type: str, app: Application):
    """Fecha a posição simulada (por Stop Loss ou Trailing Stop)."""
    if mint not in simulation_portfolio.get("positions", {}):
        return

    pos = simulation_portfolio["positions"].pop(mint)
    sale_value = pos["amount_tokens"] * current_price
    pnl_usd = sale_value - pos["invested_usd"]
    pnl_pct = (pnl_usd / pos["invested_usd"]) * 100.0

    simulation_portfolio["cash_usd"] += sale_value
    trade_record = {
        "mint": mint,
        "symbol": pos["symbol"],
        "name": pos["name"],
        "entry_price": pos["entry_price"],
        "exit_price": current_price,
        "invested_usd": pos["invested_usd"],
        "sale_value": sale_value,
        "pnl_usd": pnl_usd,
        "pnl_pct": pnl_pct,
        "close_type": close_type,
        "closed_at": time.time(),
        "duration_min": (time.time() - pos["opened_at"]) / 60.0
    }
    simulation_portfolio.setdefault("closed_trades", []).append(trade_record)
    save_simulation()

    log.info("🏁 SIMULAÇÃO FECHADA [%s]: %s PnL = $%.2f (%.1f%%) | Caixa = $%.2f", close_type, pos['symbol'], pnl_usd, pnl_pct, simulation_portfolio["cash_usd"])

    if TELEGRAM_CHAT_ID:
        try:
            stop_price = pos.get("trailing_stop") if close_type == "TRAILING_STOP" else pos.get("hard_stop")
            gap_line = ""
            if stop_price and current_price < stop_price * 0.9:
                gap_pct = (current_price / stop_price - 1.0) * 100.0
                gap_line = (
                    f"⚠️ <b>Preço lido na saída (${current_price:.8f}) ficou {abs(gap_pct):.0f}% abaixo do stop "
                    f"(${stop_price:.8f}).</b> A queda foi mais rápida que o intervalo de checagem.\n"
                )
            peak_gain = ((pos["peak_price"] - pos["entry_price"]) / pos["entry_price"]) * 100.0
            if close_type == "STOP_LOSS":
                title = f"🔴 <b>SIMULAÇÃO: STOP LOSS ACIONADO</b>"
                footer = "<i>Posição encerrada para preservar o capital restante.</i>"
            elif pnl_usd >= 0:
                title = "🎯 <b>SIMULAÇÃO: TRAILING STOP / LUCRO CONSOLIDADO</b>"
                footer = f"<i>Posição encerrada após recuo de {SIM_TRAILING_STOP_PCT:g}% do topo para garantir o lucro.</i>"
            else:
                title = "🟠 <b>SIMULAÇÃO: TRAILING STOP (SAÍDA COM PREJUÍZO)</b>"
                footer = "<i>O preço bateu no stop abaixo do preço médio de entrada, então a saída foi com perda.</i>"
            icon = "🟢" if pnl_usd >= 0 else "🔴"
            msg = (
                f"{title}\n\n"
                f"🪙 <b>Token:</b> {pos['name']} (${pos['symbol']})\n"
                f"🚀 <b>Pico Máximo Atingido:</b> {peak_gain:+.1f}% (sobre a entrada)\n"
                f"{icon} <b>Resultado Realizado:</b> {fmt_pnl(pnl_usd)} ({pnl_pct:+.1f}%)\n"
                f"🏷️ <b>Preço de Saída:</b> ${current_price:.8f} (Entrada: ${pos['entry_price']:.8f})\n"
                f"💵 <b>Retorno da Venda:</b> ${sale_value:.2f} USD\n"
                f"{gap_line}"
                f"💰 <b>Saldo Atual em Caixa:</b> ${simulation_portfolio['cash_usd']:.2f} USD\n"
                f"{footer}"
            )
            await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg, parse_mode="HTML")
        except Exception as e:
            log.warning("Falha ao enviar mensagem de fechamento simulado: %s", e)

async def simulation_monitor_loop(app: Application):
    """
    Monitora continuamente todas as posições abertas na simulação:
    - Atualiza pico máximo (ATH da posição).
    - Ativa Trailing Stop dinâmico quando a moeda sobe >= 20%.
    - Encerra se cair 20% abaixo do pico ou bater no stop loss inicial.
    - Avalia recompras adicionais na alta (DCA up).
    """
    if not ENABLE_SIMULATION:
        log.info("Simulação desativada por configuração.")
        return

    log.info("Iniciando monitoramento de posições de simulação (Trailing Stop)...")
    while True:
        try:
            positions = list(simulation_portfolio.get("positions", {}).items())
            for mint, pos in positions:
                data = await get_json(f"{DEX_BASE}/latest/dex/tokens/{mint}")
                pairs = data.get("pairs") if isinstance(data, dict) else None
                if not pairs:
                    await asyncio.sleep(0.5)
                    continue

                pairs = [p for p in pairs if p.get("chainId") == "solana"]
                if not pairs:
                    continue

                pair = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0) + float((p.get("volume") or {}).get("h1") or 0))
                current_price = get_token_price_from_pair(pair)

                if current_price <= 0:
                    continue

                prev_price = pos.get("current_price") or current_price
                pos["current_price"] = current_price

                # 1. Atualiza Pico Máximo (ATH)
                if current_price > pos.get("peak_price", 0.0):
                    pos["peak_price"] = current_price

                # 2. Ativação do Trailing Stop (+20% de lucro alcançado)
                if current_price >= pos["entry_price"] * (1.0 + SIM_STOP_LOSS_PCT / 100.0):
                    pos["trailing_active"] = True
                    calculated_trailing = pos["peak_price"] * (1.0 - SIM_TRAILING_STOP_PCT / 100.0)
                    if calculated_trailing > pos.get("trailing_stop", 0.0):
                        pos["trailing_stop"] = calculated_trailing

                # 3. Verificação de Fechamento por Trailing Stop
                if pos.get("trailing_active") and current_price <= pos.get("trailing_stop", 0.0):
                    confirmed = await confirm_exit_price(mint, pos, current_price, prev_price, pos.get("trailing_stop", 0.0))
                    if confirmed is None:
                        continue
                    await sim_close_position(mint, confirmed, "TRAILING_STOP", app)
                    continue

                # 4. Verificação de Fechamento por Hard Stop Loss Inicial (-20%)
                if not pos.get("trailing_active") and current_price <= pos.get("hard_stop", 0.0):
                    confirmed = await confirm_exit_price(mint, pos, current_price, prev_price, pos.get("hard_stop", 0.0))
                    if confirmed is None:
                        continue
                    await sim_close_position(mint, confirmed, "STOP_LOSS", app)
                    continue

                # 5. Avalia Reforço / Piramidagem na Alta
                await sim_addon_position(mint, current_price, app)

                await asyncio.sleep(0.5)

            save_simulation()

        except Exception as e:
            log.exception("Erro no monitoramento da simulação")

        await asyncio.sleep(15)

# ==========================================
# Utilitários de Rede (HTTP & RPC)
# ==========================================
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
    global last_rpc_error
    assert session is not None
    payload = {
        "jsonrpc": "2.0",
        "id": int(time.time() * 1000) % 100000,
        "method": method,
        "params": params
    }
    try:
        async with session.post(SOLANA_RPC_URL, json=payload, timeout=aiohttp.ClientTimeout(total=timeout_sec)) as r:
            if r.status != 200:
                body = (await r.text())[:120].replace("\n", " ")
                last_rpc_error = f"HTTP {r.status} {body}".strip()
                return None
            data = await r.json()
            if isinstance(data, dict) and data.get("error"):
                err = data["error"]
                last_rpc_error = "RPC erro: " + str(err.get("message", err) if isinstance(err, dict) else err)
                return None
            last_rpc_error = ""
            return data.get("result")
    except Exception as e:
        last_rpc_error = f"{type(e).__name__}: {e}"
        log.debug("Erro na chamada RPC %s: %s", method, e)
        return None

# ==========================================
# Detecção Precisa de Multi-Carteiras / Sybil Bundling
# ==========================================
def detect_bundled_wallets(holders: list[dict], pool_addresses: set[str]) -> tuple[bool, int, float, float]:
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
            total_pct = sum(c.get("pct", 0) for c in cluster)
            if total_pct >= 8.0:
                best_cluster = cluster

    if len(best_cluster) >= 3:
        total_cluster_pct = sum(c.get("pct", 0) for c in best_cluster)
        avg_pct = total_cluster_pct / len(best_cluster)
        return True, len(best_cluster), total_cluster_pct, avg_pct

    return False, 0, 0.0, 0.0

# ==========================================
# Auditoria de Risco On-Chain (Helius / Solana / Rugcheck)
# ==========================================
async def check_token_security(mint: str) -> SecurityReport:
    report = SecurityReport(mint=mint)
    pool_addresses = set(BURN_ADDRESSES)
    pool_addresses.add(PUMP_PROGRAM_ID)
    pool_addresses.add(RAYDIUM_AUTHORITY)

    # 1. Consulta RPC getAccountInfo
    mint_acc = await call_solana_rpc("getAccountInfo", [mint, {"encoding": "jsonParsed"}])
    if mint_acc and isinstance(mint_acc, dict):
        parsed_info = mint_acc.get("value", {}).get("data", {}).get("parsed", {}).get("info", {})
        report.mint_authority = parsed_info.get("mintAuthority")
        report.freeze_authority = parsed_info.get("freezeAuthority")
        report.mint_revoked = (report.mint_authority is None)
        report.freeze_revoked = (report.freeze_authority is None)

    # 2. Consulta Rugcheck Report
    rc_data = await get_json(f"{RUGCHECK_BASE}/tokens/{mint}/report")
    holders_list: list[dict] = []

    if rc_data and isinstance(rc_data, dict):
        report.rugcheck_score = rc_data.get("score")
        risks = rc_data.get("risks") or []
        report.rugcheck_risks = [str(r.get("name") or r.get("description") or "Risco") for r in risks[:6] if isinstance(r, dict)]

        if report.mint_authority is None and "mintAuthority" in rc_data:
            report.mint_authority = rc_data.get("mintAuthority")
            report.mint_revoked = (report.mint_authority is None)
        if report.freeze_authority is None and "freezeAuthority" in rc_data:
            report.freeze_authority = rc_data.get("freezeAuthority")
            report.freeze_revoked = (report.freeze_authority is None)

        markets = rc_data.get("markets") or []
        for m in markets:
            if isinstance(m, dict):
                lp_acc = m.get("lp", {}).get("lpAccount") or m.get("pool")
                if lp_acc:
                    pool_addresses.add(lp_acc)

        rc_holders = rc_data.get("topHolders") or []
        for h in rc_holders:
            if isinstance(h, dict):
                holders_list.append({
                    "address": h.get("address", ""),
                    "owner": h.get("owner", ""),
                    "pct": float(h.get("pct", 0) or 0),
                    "insider": bool(h.get("insider", False))
                })

    # 3. Fallback RPC para maiores contas
    if not holders_list:
        largest_accs = await call_solana_rpc("getTokenLargestAccounts", [mint])
        if largest_accs and isinstance(largest_accs, dict):
            acc_values = largest_accs.get("value") or []
            total_supply_acc = await call_solana_rpc("getTokenSupply", [mint])
            total_supply = float((total_supply_acc or {}).get("value", {}).get("uiAmount") or 1.0)

            for acc in acc_values:
                ui_amt = float(acc.get("uiAmount") or 0)
                pct = (ui_amt / total_supply * 100) if total_supply > 0 else 0
                holders_list.append({
                    "address": acc.get("address", ""),
                    "owner": "",
                    "pct": pct,
                    "insider": False
                })

    report.raw_holders_count = len(holders_list)

    user_holders = []
    for h in holders_list:
        addr = h.get("address", "")
        owner = h.get("owner", "")
        pct = h.get("pct", 0)

        if addr in pool_addresses or owner in pool_addresses:
            report.pool_holder_pct += pct
            continue

        if pct > 40.0 and len(user_holders) == 0:
            report.pool_holder_pct += pct
            continue

        user_holders.append(h)

    if user_holders:
        user_holders.sort(key=lambda x: x.get("pct", 0), reverse=True)
        report.max_single_holder_pct = user_holders[0].get("pct", 0)
        report.top_holder_address = user_holders[0].get("address", "")
        top10 = user_holders[:10]
        report.top10_holders_pct = sum(h.get("pct", 0) for h in top10)

    if ENABLE_BUNDLED_DETECTION and user_holders:
        is_bundled, count, total_pct, avg_pct = detect_bundled_wallets(user_holders, pool_addresses)
        if is_bundled:
            report.bundled_detected = True
            report.bundled_wallets_count = count
            report.bundled_total_pct = total_pct
            report.bundled_avg_pct = avg_pct

    return report

# ==========================================
# Idade do Par de Negociação
# ==========================================
def age_hours(pair: dict) -> float | None:
    created = pair.get("pairCreatedAt")
    if not created:
        return None
    try:
        return max(0.0, (time.time() * 1000 - float(created)) / 3_600_000)
    except (ValueError, TypeError):
        return None

# ==========================================
# Cálculo de Liquidez Efetiva (Pump.fun + Raydium)
# ==========================================
def calculate_effective_liquidity(pair: dict) -> tuple[float, bool]:
    raw_liq = float((pair.get("liquidity") or {}).get("usd") or 0)
    fdv = float(pair.get("fdv") or pair.get("marketCap") or 0)
    dex_id = str(pair.get("dexId") or "").lower()
    mint_addr = str(pair.get("baseToken", {}).get("address", "")).lower()

    is_pump = (dex_id == "pumpfun") or mint_addr.endswith("pump")

    if is_pump and raw_liq <= 100:
        effective_liq = (fdv * 0.22) if fdv > 0 else 0.0
        return effective_liq, True

    return raw_liq, is_pump

# ==========================================
# Pontuação Heurística com Metodologia dos Calloors
# ==========================================
def score_token(pair: dict, security: SecurityReport, social_mentions: int | None, smart_buyer: str | None = None):
    reasons = []
    risks = []
    subscores = {"liquidity": 0, "volume": 0, "txns": 0, "security": 0, "social": 0}

    eff_liq, is_pump = calculate_effective_liquidity(pair)
    vol = float((pair.get("volume") or {}).get("h1") or 0)
    tx = (pair.get("txns") or {}).get("h1") or {}
    buys, sells = int(tx.get("buys") or 0), int(tx.get("sells") or 0)
    fdv = float(pair.get("fdv") or pair.get("marketCap") or 0)
    age = age_hours(pair)
    socials = pair.get("info") or {}
    links = socials.get("socials") or []
    websites = socials.get("websites") or []

    # 1. Liquidez Efetiva (0 a 20)
    if eff_liq >= 30000:
        subscores["liquidity"] = 20
        reasons.append(f"Liquidez robusta >= US$30k ({'$' + f'{eff_liq:,.0f}'})")
    elif eff_liq >= 10000:
        subscores["liquidity"] = 16
        reasons.append(f"Liquidez saudável >= US$10k ({'$' + f'{eff_liq:,.0f}'})")
    elif eff_liq >= MIN_LIQUIDITY_USD:
        subscores["liquidity"] = 10
        reasons.append(f"Liquidez acima do piso mínimo ({'$' + f'{eff_liq:,.0f}'})")
    elif eff_liq > 0:
        subscores["liquidity"] = 5
        risks.append(f"Liquidez inicial em formação (${eff_liq:,.0f} < ${MIN_LIQUIDITY_USD:,.0f})")
    else:
        risks.append("Liquidez não estabelecida")

    # 2. Volume, Aceleração & Sweet Spot dos Calloors (0 a 25)
    ratio = vol / eff_liq if eff_liq > 0 else 0
    if vol >= 15000 and 0.2 <= ratio <= 8:
        subscores["volume"] = 25
        reasons.append(f"Volume 1h expressivo (${vol:,.0f})")
    elif vol >= 3000:
        subscores["volume"] = 16
        reasons.append(f"Volume inicial relevante (${vol:,.0f})")
    elif vol >= 800:
        subscores["volume"] = 8
    else:
        risks.append("Volume 1h tímido")

    # Bônus da Metodologia dos Calloors: Market Cap inicial ideal ($3k a $10k)
    if 2800 <= fdv <= 12000:
        reasons.append("🎯 Sweet Spot Alpha: Market Cap na faixa exata de entrada dos Calloors ($3k-$10k)")
        subscores["volume"] = min(25, subscores["volume"] + 4)

    # 3. Transações e Saldo Comprador (0 a 20)
    total_tx = buys + sells
    if total_tx >= 100 and buys > sells * 1.15:
        subscores["txns"] = 20
        reasons.append(f"Forte pressão compradora ({buys} compras vs {sells} vendas)")
    elif total_tx >= 25 and buys >= sells:
        subscores["txns"] = 14
        reasons.append(f"Transações ativas ({buys} compras / {sells} vendas)")
    elif total_tx >= 10:
        subscores["txns"] = 8
    else:
        risks.append("Poucas transações registradas")

    # 4. Segurança On-Chain & Distribuição (0 a 25)
    sec_points = 25

    if security.mint_revoked:
        reasons.append("Mint Authority revogada ✅")
    else:
        sec_points -= 15
        risks.append("🔴 Mint Authority ATIVA (Dev pode inflacionar supply)")

    if security.freeze_revoked:
        reasons.append("Freeze Authority revogada ✅")
    else:
        sec_points -= 20
        risks.append("🔴 ALERTA CRÍTICO: Freeze Authority ATIVA (Honeypot)")

    if security.max_single_holder_pct >= MAX_SINGLE_HOLDER_PCT:
        sec_points -= 18
        risks.append(f"🔴 Concentração Crítica: 1 carteira possui {security.max_single_holder_pct:.1f}% do supply (Risco Rugpull)")
    elif security.max_single_holder_pct >= 14.0:
        sec_points -= 7
        risks.append(f"⚠️ Maior carteira possui {security.max_single_holder_pct:.1f}% do supply")
    else:
        reasons.append(f"Maior carteira bem distribuída: {security.max_single_holder_pct:.1f}% ✅")

    if security.top10_holders_pct >= MAX_TOP10_HOLDERS_PCT:
        sec_points -= 8
        risks.append(f"⚠️ Top 10 carteiras concentram {security.top10_holders_pct:.1f}% do supply")
    else:
        reasons.append(f"Top 10 carteiras saudáveis ({security.top10_holders_pct:.1f}%) ✅")

    if security.bundled_detected:
        sec_points -= 12
        risks.append(
            f"⚠️ Suspeita Multi-Carteiras: {security.bundled_wallets_count} carteiras com saldos idênticos (~{security.bundled_avg_pct:.1f}%) somando {security.bundled_total_pct:.1f}%"
        )

    if security.rugcheck_risks:
        for r_name in security.rugcheck_risks[:2]:
            risks.append(f"Rugcheck: {r_name}")

    subscores["security"] = max(0, sec_points)

    # 5. Social & Metadados (0 a 10)
    if social_mentions is not None and social_mentions >= 5:
        subscores["social"] = 10
        reasons.append(f"{social_mentions} menções recentes no X")
    elif links or websites:
        subscores["social"] = 6
        reasons.append("Site/Redes oficiais cadastradas")
    else:
        risks.append("Sem redes sociais ou site informados")

    total_score = sum(subscores.values())

    # Bônus Especial se Smart Money comprou
    if smart_buyer:
        total_score += 25
        reasons.insert(0, f"⭐ SMART MONEY CALLOOR: {smart_buyer} comprou este token!")

    if not security.freeze_revoked or security.max_single_holder_pct >= 30.0:
        total_score = min(total_score, 35)

    risk_level = "BAIXO"
    if not security.freeze_revoked or security.max_single_holder_pct >= MAX_SINGLE_HOLDER_PCT:
        risk_level = "CRÍTICO"
    elif not security.mint_revoked or security.bundled_detected or subscores["security"] < 15:
        risk_level = "ALTO"
    elif risks:
        risk_level = "MÉDIO"

    stats = {
        "liquidity": eff_liq,
        "is_pump": is_pump,
        "volume1h": vol,
        "buys": buys,
        "sells": sells,
        "age_hours": age,
        "fdv": fdv,
        "subscores": subscores,
        "smart_buyer": smart_buyer
    }

    return min(100, max(0, total_score)), risk_level, reasons, risks, stats

# ==========================================
# Formatação de Mensagens do Telegram
# ==========================================
def fmt_usd(n: float | None) -> str:
    if n is None or n == 0:
        return "$0"
    if n >= 1_000_000:
        return f"${n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"${n/1_000:.1f}K"
    return f"${n:.0f}"

def pair_message(pair: dict, score: int, risk_level: str, reasons: list, risks: list, stats: dict, security: SecurityReport) -> str:
    base = pair.get("baseToken") or {}
    name = base.get("name") or "Token Desconhecido"
    symbol = base.get("symbol") or "?"
    mint = base.get("address") or security.mint
    url = pair.get("url") or f"https://dexscreener.com/solana/{mint}"
    info = pair.get("info") or {}
    sites = info.get("websites") or []
    socials = info.get("socials") or []
    website = next((x.get("url") for x in sites if x.get("url")), "")
    twitter = next((x.get("url") for x in socials if x.get("type") in ("twitter", "x")), "")
    pump_url = f"https://pump.fun/{mint}"
    rugcheck_url = f"https://rugcheck.xyz/tokens/{mint}"

    age = f"{stats['age_hours']:.1f}h" if stats.get("age_hours") is not None else "Recém-lançado"
    risk_icons = {"CRÍTICO": "⛔", "ALTO": "🔴", "MÉDIO": "🟡", "BAIXO": "🟢"}
    risk_icon = risk_icons.get(risk_level, "⚪")

    smart_header = ""
    if stats.get("smart_buyer"):
        smart_header = f"⭐ <b>SMART MONEY CALLOOR DETECTADO!</b>\n👑 <b>Trader:</b> <code>{stats['smart_buyer']}</code> comprou este token!\n\n"

    sub = stats.get("subscores", {})
    sub_text = f"💧 Liq: {sub.get('liquidity',0)}/20 | 📈 Vol: {sub.get('volume',0)}/25 | 🔄 Tx: {sub.get('txns',0)}/20 | 🛡️ Seg: {sub.get('security',0)}/25 | 🌐 Soc: {sub.get('social',0)}/10"

    reasons_text = "\n".join(f"• {x}" for x in reasons[:5]) or "• Sinais iniciais de atividade"
    risks_text = "\n".join(f"• {x}" for x in risks[:6]) or "• Nenhum risco crítico identificado nas verificações automatizadas"

    mint_auth_str = "Revogada ✅" if security.mint_revoked else "🔴 ATIVA (Risco)"
    freeze_auth_str = "Revogada ✅" if security.freeze_revoked else "🔴 ATIVA (Honeypot)"
    holder_str = f"{security.max_single_holder_pct:.1f}%"
    if security.max_single_holder_pct >= MAX_SINGLE_HOLDER_PCT:
        holder_str += " ⚠️ (ALERTA DEV/WHALE)"
    else:
        holder_str += " ✅"

    sybil_str = ""
    if security.bundled_detected:
        sybil_str = f"\n👥 <b>Alerta Multi-Carteiras:</b> {security.bundled_wallets_count} carteiras com compras idênticas (~{security.bundled_avg_pct:.1f}% cada, total {security.bundled_total_pct:.1f}%)"

    liq_label = "💧 <b>Liquidez (Bonding Curve):</b>" if stats.get("is_pump") else "💧 <b>Liquidez:</b>"

    links_parts = []
    if website: links_parts.append(f"<a href=\"{website}\">Website</a>")
    if twitter: links_parts.append(f"<a href=\"{twitter}\">X/Twitter</a>")
    links_parts.append(f"<a href=\"{pump_url}\">Pump.fun</a>")
    links_parts.append(f"<a href=\"{url}\">DexScreener</a>")
    links_parts.append(f"<a href=\"{rugcheck_url}\">Rugcheck</a>")
    links_str = " | ".join(links_parts)

    return (
        f"{smart_header}"
        f"🛰️ <b>RADAR DE MEMECOINS — SOLANA</b>\n\n"
        f"<b>{name} (${symbol})</b>\n"
        f"🎯 <b>Score: {score}/100</b> | {risk_icon} <b>Risco: {risk_level}</b>\n"
        f"<code>{sub_text}</code>\n\n"
        f"⏱️ <b>Idade do Par:</b> {age}\n"
        f"{liq_label} {fmt_usd(stats['liquidity'])} | <b>Volume 1h:</b> {fmt_usd(stats['volume1h'])}\n"
        f"🟢 <b>Compras 1h:</b> {stats['buys']} | 🔴 <b>Vendas 1h:</b> {stats['sells']}\n"
        f"🏷️ <b>FDV / Market Cap:</b> {fmt_usd(stats['fdv'])}\n\n"
        f"🛡️ <b>Auditoria On-Chain (Helius / Solana):</b>\n"
        f"• Mint Authority: {mint_auth_str}\n"
        f"• Freeze Authority: {freeze_auth_str}\n"
        f"• Maior Carteira (ex-pool): {holder_str}\n"
        f"• Top 10 Carteiras: {security.top10_holders_pct:.1f}%{sybil_str}\n\n"
        f"<b>💡 Destaques Positivos:</b>\n{reasons_text}\n\n"
        f"<b>⚠️ Alertas e Riscos:</b>\n{risks_text}\n\n"
        f"🪙 <b>Contrato:</b> <code>{mint}</code>\n\n"
        f"🔗 {links_str}\n\n"
        f"<i>Score heurístico de monitoramento; tokens recém-lançados podem perder 100% do valor. Não é recomendação de investimento.</i>"
    )

# ==========================================
# Moedas Base & Busca Social
# ==========================================
BASE_CURRENCIES = {
    "So11111111111111111111111111111111111111112",  # WSOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
    "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1",  # bSOL
    "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn",  # JitoSOL
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",  # mSOL
}

async def x_recent_mentions(query: str):
    if not ENABLE_X_SEARCH or not X_BEARER_TOKEN or not query:
        return None
    data = await get_json(
        "https://api.x.com/2/tweets/search/recent",
        headers={"Authorization": f"Bearer {X_BEARER_TOKEN}"},
        params={"query": f'"{query}" -is:retweet', "max_results": "10", "tweet.fields": "created_at,public_metrics"}
    )
    return data.get("meta", {}).get("result_count", 0) if isinstance(data, dict) else None

_x_ca_cache: dict[str, tuple[float, int]] = {}

async def x_ca_mentions(mint: str):
    """
    Conta posts recentes (últimos 7 dias, sem retweets) que contêm o endereço do contrato (CA).
    Cada post lido é cobrado pela API do X, então o resultado fica em cache.
    Retorna None se o X não estiver configurado ou a consulta falhar.
    """
    if not ENABLE_X_SEARCH or not X_BEARER_TOKEN or not mint:
        return None
    cached = _x_ca_cache.get(mint)
    if cached and time.time() - cached[0] < X_CACHE_SECONDS:
        return cached[1]
    data = await get_json(
        "https://api.x.com/2/tweets/search/recent",
        headers={"Authorization": f"Bearer {X_BEARER_TOKEN}"},
        params={"query": f'"{mint}" -is:retweet', "max_results": "10"}
    )
    if not isinstance(data, dict) or "meta" not in data:
        return None
    count = int(data["meta"].get("result_count", 0))
    _x_ca_cache[mint] = (time.time(), count)
    if len(_x_ca_cache) > 2000:
        for k in sorted(_x_ca_cache, key=lambda k: _x_ca_cache[k][0])[:500]:
            _x_ca_cache.pop(k, None)
    return count

def buy_decision(score: int, stats: dict, security: SecurityReport) -> tuple[bool, str]:
    """Regras de compra: score alto, token novo da Pump.fun, segurança básica e CA divulgado no X."""
    if score < BUY_MIN_SCORE:
        return False, f"score {score} < {BUY_MIN_SCORE}"
    if not (security.mint_revoked and security.freeze_revoked):
        return False, "mint/freeze authority ativa"
    if security.max_single_holder_pct >= MAX_SINGLE_HOLDER_PCT:
        return False, "carteira concentrada"
    if not stats.get("is_pump"):
        return False, "não é token da Pump.fun"
    age = stats.get("age_hours")
    if age is None or age > MAX_TOKEN_AGE_HOURS:
        return False, "token não é novo"
    if REQUIRE_CA_ON_X:
        m = stats.get("x_ca_mentions")
        if m is None:
            return False, "sem checagem do X (confira ENABLE_X_SEARCH e X_BEARER_TOKEN)"
        if m < X_CA_MIN_MENTIONS:
            return False, "CA não encontrado no X"
    return True, ""

def parse_wallet_trade(tx: dict, wallet_address: str):
    """
    Calcula o saldo líquido de cada token e de SOL na transação.
    Evita falsos positivos com tokens intermediários de roteamento e moedas base (USDC/SOL).
    """
    net_tokens: dict[str, float] = {}
    for tt in tx.get("tokenTransfers", []):
        mint = tt.get("mint")
        if not mint:
            continue
        amt = float(tt.get("tokenAmount") or 0)
        if tt.get("toUserAccount") == wallet_address:
            net_tokens[mint] = net_tokens.get(mint, 0.0) + amt
        if tt.get("fromUserAccount") == wallet_address:
            net_tokens[mint] = net_tokens.get(mint, 0.0) - amt

    net_sol = 0.0
    for nt in tx.get("nativeTransfers", []):
        amt = float(nt.get("amount") or 0) / 1e9
        if nt.get("toUserAccount") == wallet_address:
            net_sol += amt
        if nt.get("fromUserAccount") == wallet_address:
            net_sol -= amt

    active_mints = {m: amt for m, amt in net_tokens.items() if abs(amt) > 1e-6}
    memes_bought = {m: amt for m, amt in active_mints.items() if m not in BASE_CURRENCIES and amt > 0}
    memes_sold = {m: -amt for m, amt in active_mints.items() if m not in BASE_CURRENCIES and amt < 0}

    if memes_bought:
        mint, amt = list(memes_bought.items())[0]
        spent_sol = -net_sol if net_sol < 0 else 0
        spent_usdc = -active_mints.get("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", 0)
        val_desc = f"{spent_sol:.2f} SOL" if spent_sol > 0 else (f"${spent_usdc:.1f} USDC" if spent_usdc > 0 else "Swap")
        return "BUY", mint, amt, val_desc

    if memes_sold:
        mint, amt = list(memes_sold.items())[0]
        rec_sol = net_sol if net_sol > 0 else 0
        rec_usdc = active_mints.get("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", 0)
        val_desc = f"{rec_sol:.2f} SOL" if rec_sol > 0 else (f"${rec_usdc:.1f} USDC" if rec_usdc > 0 else "Swap")
        return "SELL", mint, amt, val_desc

    return None, None, 0, ""

# ==========================================
# Análise Completa de um Mint
# ==========================================
WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

async def get_new_signatures(address: str, last_sig: str | None):
    """
    Consulta barata (1 crédito Helius): assinaturas novas da carteira desde last_sig.
    Retorna (assinatura_mais_recente, [assinaturas_bem_sucedidas_da_mais_nova_para_a_mais_antiga]).
    Em caso de falha de rede retorna (None, []).
    """
    cfg: dict = {"limit": 15 if last_sig else 1, "commitment": "confirmed"}
    if last_sig:
        cfg["until"] = last_sig
    res = await call_solana_rpc("getSignaturesForAddress", [address, cfg])
    if not isinstance(res, list):
        return None, []
    if not res:
        return (last_sig or ""), []
    newest = res[0].get("signature")
    ok = [r["signature"] for r in res if r.get("signature") and not r.get("err")]
    return newest, ok

def parse_wallet_trade_rpc(tx: dict, wallet: str):
    """
    Versão por RPC (getTransaction jsonParsed) de parse_wallet_trade: usa a variação de saldo
    de tokens e SOL da carteira, sem precisar da API aprimorada (100 créditos por chamada).
    Retorna (ação, mint, quantidade, descrição) no mesmo formato de parse_wallet_trade.
    """
    meta = tx.get("meta") or {}
    if meta.get("err"):
        return None, None, 0, ""

    def balances(items):
        out: dict[str, float] = {}
        for b in items or []:
            if b.get("owner") == wallet and b.get("mint"):
                ui = b.get("uiTokenAmount") or {}
                out[b["mint"]] = out.get(b["mint"], 0.0) + float(ui.get("uiAmountString") or ui.get("uiAmount") or 0)
        return out

    pre = balances(meta.get("preTokenBalances"))
    post = balances(meta.get("postTokenBalances"))
    net_tokens = {m: post.get(m, 0.0) - pre.get(m, 0.0) for m in set(pre) | set(post)}

    net_sol = 0.0
    keys = ((tx.get("transaction") or {}).get("message") or {}).get("accountKeys") or []
    for i, k in enumerate(keys):
        pub = k.get("pubkey") if isinstance(k, dict) else k
        if pub == wallet:
            try:
                net_sol = (meta["postBalances"][i] - meta["preBalances"][i]) / 1e9
            except (KeyError, IndexError, TypeError):
                pass
            break
    net_sol += net_tokens.get(WSOL_MINT, 0.0)  # SOL embrulhado conta como SOL

    active = {m: a for m, a in net_tokens.items() if abs(a) > 1e-6}
    bought = {m: a for m, a in active.items() if m not in BASE_CURRENCIES and a > 0}
    sold = {m: -a for m, a in active.items() if m not in BASE_CURRENCIES and a < 0}
    usdc = net_tokens.get(USDC_MINT, 0.0)

    if bought:
        mint, amt = list(bought.items())[0]
        spent_sol = -net_sol if net_sol < 0 else 0
        spent_usdc = -usdc if usdc < 0 else 0
        desc = f"{spent_sol:.2f} SOL" if spent_sol > 0 else (f"${spent_usdc:.1f} USDC" if spent_usdc > 0 else "Swap")
        return "BUY", mint, amt, desc
    if sold:
        mint, amt = list(sold.items())[0]
        rec_sol = net_sol if net_sol > 0 else 0
        rec_usdc = usdc if usdc > 0 else 0
        desc = f"{rec_sol:.2f} SOL" if rec_sol > 0 else (f"${rec_usdc:.1f} USDC" if rec_usdc > 0 else "Swap")
        return "SELL", mint, amt, desc
    return None, None, 0, ""

async def analyze_mint(mint: str, smart_buyer: str | None = None, prefilter: bool = False):
    data = await get_json(f"{DEX_BASE}/latest/dex/tokens/{mint}")
    pairs = data.get("pairs") if isinstance(data, dict) else None

    if not pairs:
        # Se ainda não estiver indexado no DexScreener, mas foi comprado por Smart Money, monta par sintético
        if smart_buyer:
            security = await check_token_security(mint)
            synthetic_pair = {
                "chainId": "solana",
                "dexId": "pumpfun",
                "url": f"https://pump.fun/{mint}",
                "baseToken": {"address": mint, "name": "Novo Lançamento Pump.fun", "symbol": "PUMP"},
                "fdv": 8000,
                "volume": {"h1": 5000},
                "txns": {"h1": {"buys": 20, "sells": 5}},
                "info": {}
            }
            score, risk_level, reasons, risks, stats = score_token(synthetic_pair, security, None, smart_buyer=smart_buyer)
            return synthetic_pair, score, risk_level, reasons, risks, stats, security
        return None

    pairs = [p for p in pairs if p.get("chainId") == "solana"]
    if not pairs:
        return None

    pair = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0) + float((p.get("volume") or {}).get("h1") or 0))
    age = age_hours(pair)

    base = pair.get("baseToken") or {}

    # Pré-filtro (economiza chamadas RPC): primeiro calcula a nota com segurança "perfeita".
    # Isso é o teto possível da nota; se nem assim passa do mínimo, a checagem on-chain é inútil.
    if prefilter and not smart_buyer:
        optimistic = SecurityReport(mint=mint, mint_revoked=True, freeze_revoked=True)
        score, risk_level, reasons, risks, stats = score_token(pair, optimistic, 5, smart_buyer=smart_buyer)  # 5 = social máximo
        if score < MIN_SCORE or stats.get("liquidity", 0.0) < MIN_LIQUIDITY_USD:
            stats["x_ca_mentions"] = None
            return pair, score, risk_level, reasons, risks, stats, optimistic

    security = await check_token_security(mint)
    score, risk_level, reasons, risks, stats = score_token(pair, security, None, smart_buyer=smart_buyer)

    # O X é pago por post lido: só consulta se o token ainda pode alcançar a nota de compra (+4 = bônus social máximo)
    mentions = None
    if score + 4 >= BUY_MIN_SCORE:
        mentions = await x_ca_mentions(mint)
        if mentions is not None:
            score, risk_level, reasons, risks, stats = score_token(pair, security, mentions, smart_buyer=smart_buyer)
    stats["x_ca_mentions"] = mentions

    return pair, score, risk_level, reasons, risks, stats, security

# ==========================================
# Rastreador de Smart Money em Tempo Real (Helius)
# ==========================================
async def smart_wallet_tracker_loop(app: Application):
    """
    Monitora transações em tempo real das carteiras de calloors cadastradas.
    Dispara alerta prioritário no Telegram quando identificada uma nova compra.
    """
    if not HELIUS_API_KEY:
        log.info("Helius API Key não informada; rastreador de carteiras smart desativado.")
        return

    log.info("Iniciando Smart Money Wallet Tracker (RPC, 1 crédito por consulta, a cada ~%ss)...", SMART_POLL_SECONDS)
    while True:
        try:
            if not smart_wallets:
                await asyncio.sleep(8)
                continue

            for address, info in list(smart_wallets.items()):
                name = str(info.get("name", "Calloor")).replace("<", "").replace(">", "")
                last_sig = info.get("last_sig")
                st = smart_status.setdefault(address, {"fails": 0, "ok_at": 0.0, "err": "", "notified": False, "miss": 0, "last_tx_at": 0.0})

                # Consulta barata (1 crédito): só olha se há assinaturas novas
                newest_sig, new_sigs = await get_new_signatures(address, last_sig)
                if newest_sig is None:
                    st["fails"] += 1
                    st["err"] = last_rpc_error or "sem resposta"
                    if st["fails"] in (3, 30) or st["fails"] % 200 == 0:
                        log.warning("Smart Wallet %s: %s falhas seguidas ao consultar a carteira — %s", name, st["fails"], st["err"])
                    if st["fails"] >= 10 and not st["notified"] and TELEGRAM_CHAT_ID:
                        st["notified"] = True
                        try:
                            await app.bot.send_message(
                                chat_id=TELEGRAM_CHAT_ID,
                                text=f"⚠️ <b>Rastreador de carteiras com problema</b>\nNão consigo consultar a carteira <b>{name}</b>.\nMotivo: <code>{st['err'][:150].replace('<', '').replace('>', '')}</code>\nVocê NÃO receberá alertas dessa carteira até normalizar.",
                                parse_mode="HTML"
                            )
                        except Exception as e:
                            log.warning("Falha ao avisar problema do rastreador: %s", e)
                    await asyncio.sleep(1)
                    continue
                if st["fails"] >= 10:
                    log.info("Smart Wallet %s: consulta normalizada.", name)
                st.update({"fails": 0, "notified": False, "err": "", "ok_at": time.time()})

                # Primeira inicialização da carteira: registra a assinatura mais recente como baseline
                if not last_sig:
                    if newest_sig:
                        info["last_sig"] = newest_sig
                        save_smart_wallets()
                        log.info("Smart Wallet %s conectada. Baseline signature: %s...", name, newest_sig[:10])
                    await asyncio.sleep(1)
                    continue

                if new_sigs:
                    log.info("Smart Wallet %s: %s transação(ões) nova(s) detectada(s).", name, len(new_sigs))

                all_fetched = True
                for sig in reversed(new_sigs):  # da mais antiga para a mais nova
                    tx = await call_solana_rpc(
                        "getTransaction",
                        [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "commitment": "confirmed"}],
                    )
                    if not isinstance(tx, dict):
                        all_fetched = False
                        log.warning("Smart Wallet %s: não consegui ler a transação %s (%s)", name, sig[:10], last_rpc_error or "sem resposta")
                        continue

                    block_time = tx.get("blockTime")
                    if block_time and time.time() - block_time > SMART_MAX_TX_AGE_MIN * 60:
                        log.info("Smart Wallet %s: ignorando transação antiga %s (>%s min).", name, sig[:10], SMART_MAX_TX_AGE_MIN)
                        continue

                    action, target_mint, amt, val_desc = parse_wallet_trade_rpc(tx, address)
                    st["last_tx_at"] = time.time()

                    if action == "BUY" and target_mint:
                        key = f"{address}:{target_mint}"
                        if key in smart_seen:
                            continue
                        smart_seen.add(key)
                        log.info("🎯 SMART MONEY BUY: %s comprou token %s (gasto: %s)", name, target_mint[:8], val_desc)

                        try:
                            result = await analyze_mint(target_mint, smart_buyer=name)
                        except Exception as e:
                            log.warning("Falha ao analisar token comprado por %s: %s", name, e)
                            result = None

                        # Alerta SEMPRE enviado: toda compra de carteira smart gera mensagem (independe do radar)
                        if TELEGRAM_CHAT_ID:
                            header = f"👑 <b>SMART MONEY: {name} comprou</b> ({val_desc})\n\n"
                            if result:
                                pair, score, risk_level, reasons, risks, stats, security = result
                                body = pair_message(pair, score, risk_level, reasons, risks, stats, security)
                            else:
                                body = (
                                    f"🧾 <b>Contrato:</b> <code>{target_mint}</code>\n"
                                    f"🔗 <a href=\"https://dexscreener.com/solana/{target_mint}\">DexScreener</a> | "
                                    f"<a href=\"https://pump.fun/coin/{target_mint}\">Pump.fun</a>\n"
                                    f"<i>Sem dados de mercado disponíveis ainda.</i>"
                                )
                            fire_broadcast(app, header + body)
                            log.info("🚀 Alerta de Smart Money enviado ao Telegram: %s", target_mint)

                        if result:
                            pair = result[0]
                            buy_price = get_token_price_from_pair(pair)
                            tok_name = pair.get("baseToken", {}).get("name", "Meme Token")
                            tok_symbol = pair.get("baseToken", {}).get("symbol", "PUMP")
                            await sim_open_position(target_mint, tok_name, tok_symbol, buy_price, f"Smart Money ({name})", app)

                        seen.add(target_mint)
                        save_seen()

                    elif action == "SELL" and target_mint:
                        log.info("💰 SMART MONEY SELL: %s vendeu token %s (recebeu: %s)", name, target_mint[:8], val_desc)
                    else:
                        log.info("Smart Wallet %s: transação %s não é compra/venda de memecoin (ignorada).", name, sig[:10])

                # Só avança o marcador se todas as transações foram lidas (senão tenta de novo no próximo ciclo)
                if all_fetched or st["miss"] >= 3:
                    st["miss"] = 0
                    if info.get("last_sig") != newest_sig:
                        info["last_sig"] = newest_sig
                        save_smart_wallets()  # persiste: após reiniciar o bot não perde compras do intervalo
                else:
                    st["miss"] += 1

                await asyncio.sleep(1)

        except Exception as e:
            log.exception("Erro no rastreador de smart money")

        await asyncio.sleep(max(2, SMART_POLL_SECONDS - len(smart_wallets)))

# ==========================================
# Ciclo de Vida dos Candidatos (Priority 2)
# ==========================================
async def candidate_lifecycle_loop(app: Application):
    log.info("Iniciando loop do ciclo de vida de candidatos...")
    while True:
        try:
            now = time.time()
            active_mints = list(candidate_pool.keys())

            for mint in active_mints:
                candidate = candidate_pool.get(mint)
                if not candidate:
                    continue

                if mint in seen and mint not in buy_watch:
                    candidate.status = "ALERTED"
                    continue

                time_since_last = now - candidate.last_checked_at
                required_interval = 20 if candidate.check_count == 0 else (60 if candidate.check_count == 1 else 240)

                if time_since_last < required_interval:
                    continue

                candidate.check_count += 1
                candidate.last_checked_at = now

                token_age_h = (now - candidate.detected_at) / 3600.0
                if token_age_h > MAX_TOKEN_AGE_HOURS:
                    candidate.status = "EXPIRED"
                    buy_watch.discard(mint)
                    continue

                result = await analyze_mint(mint, prefilter=True)
                if not result:
                    candidate.status = "OBSERVING"
                    await asyncio.sleep(0.3)
                    continue

                pair, score, risk_level, reasons, risks, stats, security = result
                candidate.last_score = score
                candidate.last_liquidity = stats.get("liquidity", 0.0)
                candidate.last_volume = stats.get("volume1h", 0.0)

                log.info(
                    "Candidato: %s (%s) score=%s/100 liq_est=$%.0f risco=%s checagem=#%s",
                    candidate.symbol or pair.get("baseToken", {}).get("symbol", "?"),
                    mint[:6], score, candidate.last_liquidity, risk_level, candidate.check_count
                )

                if score >= MIN_SCORE and candidate.last_liquidity >= MIN_LIQUIDITY_USD and TELEGRAM_CHAT_ID:
                    # O alerta é enviado uma única vez
                    if mint not in seen:
                        msg = pair_message(pair, score, risk_level, reasons, risks, stats, security)
                        fire_broadcast(app, msg)
                        seen.add(mint)
                        save_seen()
                        log.info("🚀 ALERTA ENVIADO para %s (${%s}) Score=%s Risco=%s", mint, candidate.symbol, score, risk_level)

                    # A compra simulada só acontece quando TODAS as regras de compra forem atendidas
                    ok, why = buy_decision(score, stats, security)
                    if ok:
                        buy_price = get_token_price_from_pair(pair)
                        tok_name = pair.get("baseToken", {}).get("name", candidate.name or "Meme Token")
                        tok_symbol = pair.get("baseToken", {}).get("symbol", candidate.symbol or "PUMP")
                        reason = f"Radar Score {score}/100 + CA no X ({stats.get('x_ca_mentions')})" if REQUIRE_CA_ON_X else f"Radar Score {score}/100"
                        await sim_open_position(mint, tok_name, tok_symbol, buy_price, reason, app)

                    if ok and mint in simulation_portfolio.get("positions", {}):
                        buy_watch.discard(mint)
                        candidate.status = "ALERTED"
                    else:
                        # Continua observando até o token envelhecer: o CA pode ser postado no X depois
                        buy_watch.add(mint)
                        candidate.status = "OBSERVING"
                        if not ok:
                            log.info("Sem compra em %s: %s", candidate.symbol or mint[:6], why)
                else:
                    candidate.status = "OBSERVING"

                await asyncio.sleep(0.4)

            if len(candidate_pool) > 1000:
                expired_keys = [k for k, v in candidate_pool.items() if v.status in ("ALERTED", "EXPIRED", "DISCARDED")]
                for k in expired_keys[:200]:
                    candidate_pool.pop(k, None)

        except Exception:
            log.exception("Erro no ciclo de vida de candidatos")

        await asyncio.sleep(POLL_SECONDS)

# ==========================================
# Ingestão em Tempo Real: PumpPortal WebSocket
# ==========================================
async def pumpportal_listener():
    if not ENABLE_PUMPPORTAL:
        return

    log.info("Iniciando escuta em tempo real do PumpPortal WebSocket (%s)...", PUMPPORTAL_WS_URL)
    while True:
        try:
            async with websockets.connect(PUMPPORTAL_WS_URL, ping_interval=20, close_timeout=10) as ws:
                subscribe_msg = {"method": "subscribeNewToken"}
                await ws.send(json.dumps(subscribe_msg))
                log.info("Inscrito com sucesso em novos tokens do Pump.fun via PumpPortal.")

                async for raw_msg in ws:
                    try:
                        data = json.loads(raw_msg)
                        mint = data.get("mint")
                        if not mint or mint in seen:
                            continue

                        if mint not in candidate_pool:
                            symbol = data.get("symbol", "")
                            name = data.get("name", "")
                            initial_buy = data.get("initialBuy")
                            candidate_pool[mint] = CandidateToken(
                                mint=mint,
                                source="pump_portal",
                                detected_at=time.time(),
                                name=name,
                                symbol=symbol,
                                initial_buy_sol=float(initial_buy) if initial_buy else None,
                                status="DETECTED"
                            )
                            log.info("⚡ Novo token Pump.fun: %s ($%s) mint=%s", name, symbol, mint[:8])
                    except Exception as e:
                        log.debug("Erro processando mensagem PumpPortal: %s", e)

        except Exception as e:
            log.warning("PumpPortal desconectado (%s). Reconectando em 5 segundos...", e)
            await asyncio.sleep(5)

# ==========================================
# Descoberta Contínua via DexScreener
# ==========================================
async def dexscreener_discovery_loop():
    while True:
        try:
            for endpoint in ("/token-profiles/latest/v1", "/token-boosts/latest/v1"):
                data = await get_json(DEX_BASE + endpoint)
                if isinstance(data, list):
                    for item in data:
                        if item.get("chainId") == "solana" and item.get("tokenAddress"):
                            mint = item["tokenAddress"]
                            if mint not in seen and mint not in candidate_pool:
                                candidate_pool[mint] = CandidateToken(
                                    mint=mint,
                                    source="dexscreener",
                                    detected_at=time.time(),
                                    status="DETECTED"
                                )
                await asyncio.sleep(1.5)
        except Exception as e:
            log.debug("Erro na descoberta DexScreener: %s", e)

        await asyncio.sleep(POLL_SECONDS)

# ==========================================
# Comandos do Telegram
# ==========================================
# ==========================================
# Assinantes, administrador e envio de alertas para todos
# ==========================================
def load_subscribers():
    global subscribers
    try:
        with open(SUBSCRIBERS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        subscribers = data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError):
        subscribers = {}

def save_subscribers():
    try:
        with open(SUBSCRIBERS_FILE, "w", encoding="utf-8") as f:
            json.dump(subscribers, f)
    except OSError as e:
        log.warning("Falha ao salvar %s: %s", SUBSCRIBERS_FILE, e)

def is_admin(update: Update) -> bool:
    chat = update.effective_chat
    return bool(TELEGRAM_CHAT_ID) and chat is not None and str(chat.id) == str(TELEGRAM_CHAT_ID)

def admin_only(fn):
    """Comandos de administração (simulação, carteiras, configuração) só funcionam no chat do dono."""
    @functools.wraps(fn)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not is_admin(update):
            if update.message:
                await update.message.reply_text("🔒 Este comando é restrito ao administrador do bot.")
            return
        return await fn(update, context)
    return wrapper

async def broadcast_alert(app: Application, text: str) -> int:
    """Envia o alerta ao administrador (primeiro) e a todos os assinantes. Remove quem bloqueou o bot."""
    admin = str(TELEGRAM_CHAT_ID) if TELEGRAM_CHAT_ID else ""
    recipients = [admin] if admin else []
    if BROADCAST_ALERTS:
        recipients += [c for c in list(subscribers) if c != admin]

    delivered = 0
    for chat_id in recipients:
        for attempt in range(2):
            try:
                await app.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", disable_web_page_preview=True)
                delivered += 1
                break
            except RetryAfter as e:
                await asyncio.sleep(float(e.retry_after) + 1)  # limite do Telegram: espera e tenta de novo uma vez
            except Forbidden:
                if chat_id != admin and subscribers.pop(chat_id, None) is not None:
                    save_subscribers()
                    log.info("Assinante %s bloqueou o bot e foi removido.", chat_id)
                break
            except BadRequest as e:
                if chat_id != admin and "chat not found" in str(e).lower() and subscribers.pop(chat_id, None) is not None:
                    save_subscribers()
                else:
                    log.warning("Falha ao enviar alerta para %s: %s", chat_id, e)
                break
            except Exception as e:
                log.warning("Falha ao enviar alerta para %s: %s", chat_id, e)
                break
        await asyncio.sleep(0.05)  # ~20 mensagens/s, abaixo do limite do Telegram
    return delivered

def fire_broadcast(app: Application, text: str):
    """Dispara o envio em segundo plano para não travar o radar quando há muitos assinantes."""
    task = asyncio.create_task(broadcast_alert(app, text))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if is_admin(update):
        msg = (
            "🛰️ <b>Solana Memecoin Radar Ativo!</b>\n\n"
            f"🎯 <b>Score Mínimo:</b> {MIN_SCORE}/100\n"
            f"🛒 <b>Score p/ Compra:</b> {BUY_MIN_SCORE}/100 | CA no X: {'exigido' if REQUIRE_CA_ON_X else 'opcional'}\n"
            f"💧 <b>Liquidez Mínima:</b> ${MIN_LIQUIDITY_USD:,.0f}\n"
            f"⏱️ <b>Idade Máxima:</b> {MAX_TOKEN_AGE_HOURS:g} horas\n"
            f"🛡️ <b>Teto Máx. Dev:</b> {MAX_SINGLE_HOLDER_PCT}%\n"
            f"👑 <b>Smart Wallets Monitoradas:</b> {len(smart_wallets)}\n"
            f"🔑 <b>Helius RPC:</b> {'Conectado ✅' if HELIUS_API_KEY else 'RPC Padrão'}\n"
            f"👥 <b>Inscritos nos alertas:</b> {len(subscribers)} ({'envio para todos ligado' if BROADCAST_ALERTS else 'envio para todos desligado'})\n\n"
            "Comandos disponíveis:\n"
            "/radar - Exibe status do radar e candidatos em observação\n"
            "/portfolio ou /sim - Acompanha sua carteira de simulação (Paper Trading)\n"
            "/reset_sim - Reinicia a simulação com saldo de $20.00\n"
            "/wallets - Lista carteiras de Smart Money em monitoramento\n"
            "/addwallet <code>&lt;nome&gt; &lt;endereco&gt;</code> - Adiciona carteira de calloor para rastrear\n"
            "/delwallet <code>&lt;endereco&gt;</code> - Remove carteira de calloor\n"
            "/status - Configurações detalhadas do sistema\n"
            "/subscribers - Quantidade de inscritos nos alertas\n"
            "/check <code>&lt;mint&gt;</code> - Realiza auditoria on-chain imediata de qualquer token"
        )
        await update.message.reply_text(msg, parse_mode="HTML")
        return

    cid = str(chat.id)
    if not BROADCAST_ALERTS:
        status_line = "ℹ️ O envio automático de alertas está desligado no momento."
    elif cid in subscribers:
        status_line = "✅ Você já está inscrito e continua recebendo os alertas."
    elif len(subscribers) >= MAX_SUBSCRIBERS:
        status_line = "⚠️ O limite de inscritos foi atingido. Tente novamente mais tarde."
    else:
        subscribers[cid] = {"since": int(time.time())}
        save_subscribers()
        status_line = "✅ Pronto! Você passou a receber os alertas de memecoins novas e de compras das carteiras monitoradas."
        log.info("Novo assinante: %s (total: %s)", cid, len(subscribers))

    msg = (
        "🛰️ <b>Solana Memecoin Radar</b>\n\n"
        f"{status_line}\n\n"
        f"Os alertas saem quando um token novo atinge score de pelo menos {MIN_SCORE}/100, e quando as carteiras de calloors monitoradas compram um token.\n\n"
        "Comandos:\n"
        "/radar - Candidatos em observação agora\n"
        "/check <code>&lt;mint&gt;</code> - Auditoria rápida de qualquer token\n"
        "/stop - Parar de receber alertas\n\n"
        "<i>⚠️ Os alertas são heurísticos e servem só para monitoramento. Não são recomendação de investimento: "
        "tokens recém-lançados podem perder 100% do valor. Decida por conta própria.</i>"
    )
    await update.message.reply_text(msg, parse_mode="HTML")

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_admin(update):
        await update.message.reply_text("Você é o administrador e sempre recebe os alertas.")
        return
    if subscribers.pop(str(update.effective_chat.id), None) is not None:
        save_subscribers()
        await update.message.reply_text("🔕 Pronto, você não receberá mais alertas. Envie /start para voltar a receber.")
    else:
        await update.message.reply_text("Você não estava inscrito nos alertas. Envie /start para se inscrever.")

async def chatid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mostra o ID do chat atual (útil para configurar HUNTER_X_CHAT_ID)."""
    await update.message.reply_text(f"ID deste chat: <code>{update.effective_chat.id}</code>", parse_mode="HTML")

@admin_only
async def subscribers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"👥 Inscritos nos alertas: <b>{len(subscribers)}</b> (limite {MAX_SUBSCRIBERS})\n"
        f"Envio para todos: {'ligado ✅' if BROADCAST_ALERTS else 'desligado'}",
        parse_mode="HTML",
    )

@admin_only
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    observing = sum(1 for c in candidate_pool.values() if c.status == "OBSERVING")
    detected = sum(1 for c in candidate_pool.values() if c.status == "DETECTED")
    msg = (
        f"📊 <b>STATUS DO RADAR</b>\n\n"
        f"• Tokens já alertados: {len(seen)}\n"
        f"• Candidatos em observação ativa: {observing}\n"
        f"• Candidatos recém-detectados: {detected}\n"
        f"• Pool total em memória: {len(candidate_pool)}\n"
        f"• Carteiras Smart Money monitoradas: {len(smart_wallets)}\n"
        f"• Helius API: {'Ativa ✅' if HELIUS_API_KEY else 'Inativa (usando RPC público)'}\n"
        f"• Detecção Multi-Carteiras: {'Ativa ✅' if ENABLE_BUNDLED_DETECTION else 'Desativada'}\n"
        f"• Intervalo de Varredura: {POLL_SECONDS}s"
    )
    await update.message.reply_text(msg, parse_mode="HTML")

async def radar_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    recent_candidates = [
        c for c in candidate_pool.values()
        if c.status in ("OBSERVING", "DETECTED")
    ][-6:]

    lines = []
    for c in reversed(recent_candidates):
        symbol = c.symbol or "Token"
        lines.append(f"• <b>${symbol}</b> (<code>{c.mint[:6]}...</code>) | Score: {c.last_score}/100 | Liq: {fmt_usd(c.last_liquidity)}")

    candidates_text = "\n".join(lines) if lines else "Nenhum candidato em análise no momento."

    msg = (
        f"👀 <b>RADAR EM TEMPO REAL</b>\n\n"
        f"<b>Últimos candidatos no funil:</b>\n{candidates_text}\n\n"
        f"<i>O bot avalia liquidez, volume, mint/freeze authority e multi-carteiras antes de emitir alertas.</i>"
    )
    await update.message.reply_text(msg, parse_mode="HTML")

async def check_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Uso: <code>/check &lt;endereço_do_contrato&gt;</code>", parse_mode="HTML")
        return

    mint = context.args[0].strip()
    if not MINT_RE.match(mint):
        await update.message.reply_text("❌ Endereço de contrato inválido. Envie o endereço completo do token (CA).")
        return
    if not is_admin(update):
        chat_id = update.effective_chat.id
        wait = CHECK_COOLDOWN_SECONDS - (time.time() - _check_last.get(chat_id, 0.0))
        if wait > 0:
            await update.message.reply_text(f"⏳ Aguarde {wait:.0f}s para usar /check novamente.")
            return
        _check_last[chat_id] = time.time()
    await update.message.reply_text(f"🔍 Auditando token <code>{mint}</code>...", parse_mode="HTML")

    try:
        result = await analyze_mint(mint)
        if not result:
            await update.message.reply_text("❌ Não foram encontrados pares de negociação Solana no DexScreener para este contrato.")
            return

        pair, score, risk_level, reasons, risks, stats, security = result
        msg = pair_message(pair, score, risk_level, reasons, risks, stats, security)
        await update.message.reply_text(msg, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        log.exception("Erro executando /check")
        await update.message.reply_text(f"Erro ao auditar token: {e}")

@admin_only
async def wallets_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not smart_wallets:
        await update.message.reply_text(
            "Nenhuma carteira de Smart Money cadastrada ainda.\n"
            "Use: <code>/addwallet &lt;nome&gt; &lt;endereco_solana&gt;</code>",
            parse_mode="HTML"
        )
        return

    lines = []
    for addr, info in smart_wallets.items():
        name = info.get("name", "Calloor")
        st = smart_status.get(addr)
        if not st or not st.get("ok_at") and not st.get("fails"):
            status = "⏳ aguardando 1ª consulta"
        elif st.get("fails"):
            status = f"❌ falhando ({st['fails']}x): {str(st.get('err',''))[:80].replace('<','').replace('>','')}"
        else:
            status = f"✅ ok (última consulta há {int(time.time() - st['ok_at'])}s)"
        lines.append(f"• <b>{name}</b>: <code>{addr}</code>\n   {status}")

    msg = f"👑 <b>CARTEIRAS SMART MONEY MONITORADAS ({len(smart_wallets)}):</b>\n\n" + "\n".join(lines)
    await update.message.reply_text(msg, parse_mode="HTML")

@admin_only
async def addwallet_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text(
            "Uso: <code>/addwallet &lt;nome&gt; &lt;endereco_solana&gt;</code>\n"
            "Exemplo: <code>/addwallet buddhaaaaa 3VUN...o9vC</code>",
            parse_mode="HTML"
        )
        return

    name = context.args[0].strip()
    address = context.args[1].strip()

    smart_wallets[address] = {"name": name, "added_at": time.time()}
    save_smart_wallets()

    await update.message.reply_text(
        f"✅ Carteira <b>{name}</b> adicionada com sucesso!\n"
        f"Endereço: <code>{address}</code>\n"
        "O bot agora avisará imediatamente no Telegram quando este calloor comprar qualquer token!",
        parse_mode="HTML"
    )
    log.info("Smart wallet adicionada: %s (%s)", name, address)

@admin_only
async def delwallet_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Uso: <code>/delwallet &lt;endereco_solana&gt;</code>", parse_mode="HTML")
        return

    address = context.args[0].strip()
    if address in smart_wallets:
        name = smart_wallets[address].get("name", "")
        del smart_wallets[address]
        save_smart_wallets()
        await update.message.reply_text(f"🗑️ Carteira <b>{name}</b> (<code>{address}</code>) removida do monitoramento.", parse_mode="HTML")
    else:
        await update.message.reply_text("Carteira não encontrada na lista.", parse_mode="HTML")

@admin_only
async def portfolio_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cash = simulation_portfolio.get("cash_usd", 0.0)
    init_cap = simulation_portfolio.get("initial_capital", SIM_INITIAL_CAPITAL)
    positions = simulation_portfolio.get("positions", {})
    closed = simulation_portfolio.get("closed_trades", [])

    total_invested = sum(p.get("invested_usd", 0.0) for p in positions.values())
    current_pos_val = sum(p.get("amount_tokens", 0.0) * p.get("current_price", p.get("entry_price", 0.0)) for p in positions.values())
    total_equity = cash + current_pos_val
    total_pnl_usd = total_equity - init_cap
    total_pnl_pct = (total_pnl_usd / init_cap) * 100.0 if init_cap > 0 else 0.0

    pos_lines = []
    for mint, p in positions.items():
        cur_val = p.get("amount_tokens", 0.0) * p.get("current_price", p.get("entry_price", 0.0))
        pnl = cur_val - p.get("invested_usd", 0.0)
        pnl_p = (pnl / p.get("invested_usd", 1.0)) * 100.0
        pnl_icon = "🟢" if pnl >= 0 else "🔴"
        trailing_tag = "🎯 Trailing Ativo" if p.get("trailing_active") else "🛑 Stop Fixo"
        pos_lines.append(
            f"• <b>${p['symbol']}</b>: ${p['invested_usd']:.2f} ➔ ${cur_val:.2f} ({pnl_icon} <b>{pnl_p:+.1f}%</b>)\n"
            f"  🏷️ Entrada: ${p['entry_price']:.8f} | Pico: ${p['peak_price']:.8f}\n"
            f"  🛑 Stop: ${p['trailing_stop']:.8f} ({trailing_tag})"
        )

    pos_text = "\n".join(pos_lines) if pos_lines else "Nenhuma posição aberta no momento."

    closed_lines = []
    for c in reversed(closed[-4:]):
        icon = "🟢" if c.get("pnl_usd", 0) >= 0 else "🔴"
        closed_lines.append(f"• <b>${c.get('symbol')}</b>: {icon} ${c.get('pnl_usd'):+.2f} ({c.get('pnl_pct'):+.1f}%) [{c.get('close_type')}]")
    closed_text = "\n".join(closed_lines) if closed_lines else "Nenhum trade encerrado ainda."

    pnl_header_icon = "🚀" if total_pnl_usd >= 0 else "🔻"
    msg = (
        f"💼 <b>PORTFÓLIO DE SIMULAÇÃO (PAPER TRADING)</b>\n\n"
        f"💰 <b>Saldo em Caixa:</b> ${cash:.2f} USD\n"
        f"📊 <b>Valor em Posições:</b> ${current_pos_val:.2f} USD\n"
        f"🏦 <b>Patrimônio Total:</b> ${total_equity:.2f} USD ({pnl_header_icon} <b>{total_pnl_pct:+.1f}%</b>)\n"
        f"💵 <b>Capital Inicial:</b> ${init_cap:.2f} USD\n\n"
        f"<b>Posições Abertas ({len(positions)}):</b>\n{pos_text}\n\n"
        f"<b>Últimos Fechamentos:</b>\n{closed_text}\n\n"
        f"<i>Compras automáticas de $2.00 por token | Stop Loss: -20% | Trailing Stop: 20% do pico.</i>"
    )
    await update.message.reply_text(msg, parse_mode="HTML")

@admin_only
async def reset_sim_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global simulation_portfolio
    simulation_portfolio = {
        "initial_capital": SIM_INITIAL_CAPITAL,
        "cash_usd": SIM_INITIAL_CAPITAL,
        "positions": {},
        "closed_trades": []
    }
    save_simulation()
    await update.message.reply_text(
        f"🔄 <b>Simulação Reiniciada com Sucesso!</b>\n\n"
        f"💰 Saldo restaurado para: <b>${SIM_INITIAL_CAPITAL:.2f} USD</b>\n"
        f"Todas as posições anteriores e histórico foram zerados.",
        parse_mode="HTML"
    )

# ==========================================
# Ciclo de Vida da Aplicação
# ==========================================
async def post_init(app: Application):
    global session
    session = aiohttp.ClientSession(headers={"User-Agent": "MemecoinRadar/2.0"})

    load_smart_wallets()
    load_simulation()
    load_subscribers()

    if TELEGRAM_CHAT_ID:
        try:
            init_msg = (
                "🛰️ <b>Radar de Memecoins Conectado!</b>\n\n"
                "O bot está online e monitorando novos lançamentos no ecossistema Solana e Pump.fun.\n"
                f"🎯 Score Mínimo: {MIN_SCORE}/100 | 💧 Liquidez Mínima: ${MIN_LIQUIDITY_USD:,.0f}\n"
                f"👑 Carteiras Smart Money: {len(smart_wallets)} ativas\n"
                f"👥 Inscritos nos alertas: {len(subscribers)} (envio para todos {'ligado' if BROADCAST_ALERTS else 'desligado'})\n"
                f"💼 Simulação Ativa: Caixa ${simulation_portfolio['cash_usd']:.2f} USD (Stop: -{SIM_STOP_LOSS_PCT:g}% | Trailing: {SIM_TRAILING_STOP_PCT:g}%)\n\n"
                "Use /radar para ver tokens em observação ou /portfolio para ver sua carteira simulada."
            )
            await app.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=init_msg,
                parse_mode="HTML"
            )
            log.info("Mensagem de inicialização enviada ao Telegram.")
        except Exception as e:
            log.warning("Falha ao enviar mensagem de inicialização no Telegram: %s", e)

    app.bot_data["tasks"] = [
        app.create_task(candidate_lifecycle_loop(app)),
        app.create_task(pumpportal_listener()),
        app.create_task(dexscreener_discovery_loop()),
        app.create_task(smart_wallet_tracker_loop(app)),
        app.create_task(simulation_monitor_loop(app)),
    ]
    log.info("Radar inicializado com sucesso e tarefas em segundo plano ativadas (incluindo Simulação).")

async def post_shutdown(app: Application):
    global session
    if session:
        await session.close()
    log.info("Radar encerrado.")

# ==========================================
# Ponto de Entrada Principal
# ==========================================
async def run_app(app: Application):
    """
    Executa o bot sem depender do run_polling, que encerra em silêncio (só log de debug)
    quando recebe SystemExit/KeyboardInterrupt e que mexe no loop de eventos de forma
    diferente entre versões do Python.
    """
    await app.initialize()
    try:
        await app.start()
        await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        await post_init(app)
        log.info("Bot em execução e escutando comandos. Pressione Ctrl+C para encerrar.")
        await asyncio.Event().wait()
    finally:
        tasks = app.bot_data.get("tasks", [])
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            if app.updater and app.updater.running:
                await app.updater.stop()
            if app.running:
                await app.stop()
        except Exception:
            log.exception("Erro ao parar a conexão com o Telegram")
        await post_shutdown(app)
        await app.shutdown()

def main():
    if not TELEGRAM_BOT_TOKEN:
        raise SystemExit("Configure TELEGRAM_BOT_TOKEN no arquivo .env")

    load_seen()
    app = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("stop", stop_command))
    app.add_handler(CommandHandler("subscribers", subscribers_command))
    app.add_handler(CommandHandler("chatid", chatid_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("radar", radar_command))
    app.add_handler(CommandHandler("check", check_command))
    app.add_handler(CommandHandler("wallets", wallets_command))
    app.add_handler(CommandHandler("addwallet", addwallet_command))
    app.add_handler(CommandHandler("delwallet", delwallet_command))
    app.add_handler(CommandHandler("portfolio", portfolio_command))
    app.add_handler(CommandHandler("sim", portfolio_command))
    app.add_handler(CommandHandler("reset_sim", reset_sim_command))

    log.info("Python %s | python-telegram-bot %s", sys.version.split()[0], telegram.__version__)
    log.info("Iniciando radar de memecoins Solana. Score mínimo configurado: %s", MIN_SCORE)
    try:
        asyncio.run(run_app(app))
    except KeyboardInterrupt:
        log.info("Radar encerrado pelo usuário (Ctrl+C).")
    except BaseException:
        log.exception("O radar parou por causa de um erro:")
        raise SystemExit(1)
    else:
        log.warning("O radar terminou sem erro, o que não era esperado.")

if __name__ == "__main__":
    main()
