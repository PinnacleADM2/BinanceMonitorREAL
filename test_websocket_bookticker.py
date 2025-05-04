# -*- coding: utf-8 -*-
import time
from binance import ThreadedWebsocketManager
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_KEY = None
API_SECRET = None
SYMBOL = 'btcusdt' # You can change this symbol

def handle_message(msg):
    if 'stream' in msg and 'data' in msg:
        print(f"BOOKTICKER Received ({msg['stream']}): {msg['data']}")
    else:
        print(f"BOOKTICKER Received (raw): {msg}")

if __name__ == "__main__":
    print(f"--- WebSocket Test Script: Book Ticker ({SYMBOL}) ---")
    stream_to_start = f'{SYMBOL.lower()}@bookTicker'
    print(f"Attempting to connect to {stream_to_start}...")

    twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)
    twm.start()

    stream_name = None
    try:
        # Use start_symbol_book_ticker_socket for individual book ticker
        stream_name = twm.start_symbol_book_ticker_socket(callback=handle_message, symbol=SYMBOL)

        # Alternatively, use multiplex if you prefer that structure or need multiple streams
        # stream_name = twm.start_multiplex_socket(callback=handle_message, streams=[stream_to_start])

        if stream_name:
            print(f"Stream started: {stream_name}")
            print("Waiting for messages... Run for ~5 mins, then press CTRL+C.")
            twm.join()
        else:
            print("Failed to start stream.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("\nStopping WebSocket Manager...")
        twm.stop()
        print("Script finished.") 