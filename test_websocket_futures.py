# test_futures_all_pairs_filter_only_big.py

import asyncio
import json
import logging
from decimal import Decimal

import websockets

# --- parâmetros ---
SYMBOL        = "!aggTrade@arr"           # stream de todas as trades agregadas
ENDPOINT      = f"wss://fstream.binance.com/ws/{SYMBOL}"
THRESHOLD_USD = Decimal("10000")          # filtra trades ≥ 10 000 USDT

# configure o logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S"
)

async def main():
    async with websockets.connect(ENDPOINT) as ws:
        logging.info(f"Conectado ao {ENDPOINT}")
        while True:
            raw = await ws.recv()
            msg = json.loads(raw)

            qty   = Decimal(msg["q"])
            price = Decimal(msg["p"])
            usd   = qty * price

            # só imprime se for grande
            if usd >= THRESHOLD_USD:
                logging.info(f"RAW: {msg} 💥")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Encerrando…")
