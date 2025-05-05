# -*- coding: utf-8 -*-
"""
Grandes negociações Spot – stream !ticker@arr
Agora capturamos TODOS os tickers Spot e aplicamos o filtro no quoteVolume (q)
"""
import json, datetime, logging
from decimal import Decimal
from websocket import WebSocketApp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S"
)

URL = "wss://stream.binance.com:9443/ws/!ticker@arr"
THRESHOLD_USD = Decimal("200000")

def on_message(ws, message: str):
    # message vira uma lista de objetos ticker
    tickers = json.loads(message)
    # mostra todo o conteúdo RAW
    logging.info(f"RAW: {tickers}")

    # para cada ticker, filtra pelos maiores quote volumes
    for data in tickers:
        symbol     = data["s"]
        quoteVol   = Decimal(data.get("q", "0"))  # volume em USDT no período
        if quoteVol >= THRESHOLD_USD:
            ts   = datetime.datetime.utcnow().strftime("%H:%M:%S")
            side = "💥"  # marca todos os grandes
            logging.warning(f"{side} {quoteVol:.0f} USDT   {symbol}   {ts} UTC")

def on_error(ws, error):
    logging.error("WS error: %s", error)

def on_close(ws, *_):
    logging.info("WebSocket closed – reconectando em 5 s")
    ws.run_forever(ping_interval=20, ping_timeout=10)

def on_open(ws):
    logging.info("Connected to Binance Spot !ticker@arr 🚀")

def main():
    ws = WebSocketApp(
        URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever(ping_interval=20, ping_timeout=10)

if __name__ == "__main__":
    logging.info(f"--- Só tickers Spot com quoteVolume ≥ {THRESHOLD_USD} USDT ---")
    main()
