# -*- coding: utf-8 -*-
"""
Teste de liquidações Futures (Binance) – stream !forceOrder@arr
Filtra liquidações ≥ THRESHOLD e mantém estatísticas gerais, que são exibidas ao interromper.
"""
import json, datetime, logging, time
from decimal import Decimal
from websocket import WebSocketApp

# CONFIGURAÇÃO
THRESHOLD = Decimal('500')
URL = "wss://fstream.binance.com/ws/!forceOrder@arr"

# STATÍSTICAS GLOBAIS
total_events = 0
filtered_events = 0
total_usd = Decimal('0')
buy_count = 0
sell_count = 0
symbol_counts = {}
symbol_volumes = {}

# LOGGING
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S")

logging.info(f"--- Só liquidações ≥ {THRESHOLD} USDT ---")

def print_summary():
    print("\n=== RESUMO DE LIQUIDAÇÕES ===")
    print(f"Total de eventos recebidos: {total_events}")
    print(f"Eventos filtrados (≥ {THRESHOLD} USDT): {filtered_events}")
    print(f"Volume total filtrado: {total_usd} USDT")
    avg = (total_usd / filtered_events) if filtered_events else 0
    print(f"Tamanho médio por evento filtrado: {avg:.2f} USDT")
    print(f"Direção BUY: {buy_count} | SELL: {sell_count}")
    print("\nTop símbolos por eventos e volume:")
    # montar lista de tuplas: (símbolo, contagem, volume)
    summary = []
    for sym, cnt in symbol_counts.items():
        vol = symbol_volumes.get(sym, Decimal('0'))
        summary.append((sym, cnt, vol))
    # ordenar por volume desc
    for sym, cnt, vol in sorted(summary, key=lambda x: x[2], reverse=True):
        print(f"  {sym:12} | Eventos: {cnt:4} | Volume: {vol:11.2f} USDT")
    print("=== FIM DO RESUMO ===")

# CALLBACKS DO WS

def on_message(ws, message: str):
    global total_events, filtered_events, total_usd, buy_count, sell_count
    total_events += 1

    msg = json.loads(message)
    data = msg.get("data", {})
    o = data.get("o", {})

    # quantidade e preço
    try:
        qty = Decimal(o.get("q", "0"))
        price = Decimal(o.get("p", "0"))
        usd = qty * price
    except Exception:
        usd = None

    # atualização por símbolo
    sym = o.get("s", "UNKNOWN")
    symbol_counts[sym] = symbol_counts.get(sym, 0) + 1
    if usd is not None:
        symbol_volumes[sym] = symbol_volumes.get(sym, Decimal('0')) + usd

    side = o.get("S", "?")
    ts = datetime.datetime.utcnow().strftime("%H:%M:%S")

    # filtro e estatísticas
    if usd is not None and usd >= THRESHOLD:
        filtered_events += 1
        total_usd += usd
        if side.upper() == 'BUY':
            buy_count += 1
        else:
            sell_count += 1
        logging.warning(
            f"💧  {usd:.2f} USDT   {side:<5}  {sym}   {ts} UTC"
        )
    # sempre imprimir RAW
    print("RAW:", msg)


def on_error(ws, error):
    logging.error("WS error: %s", error)


def on_close(ws, close_status_code, close_msg):
    logging.info("WebSocket closed.")


def on_open(ws):
    logging.info("Connected to Binance Futures forceOrder stream 🚀")


def main():
    ws = WebSocketApp(
        URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    try:
        ws.run_forever(ping_interval=20, ping_timeout=10)
    except KeyboardInterrupt:
        logging.info("Interrompido pelo usuário, gerando resumo...")
        print_summary()
        ws.close()

if __name__ == "__main__":
    main()
