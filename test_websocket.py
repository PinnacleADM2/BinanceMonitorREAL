# -*- coding: utf-8 -*-
import time
from binance import ThreadedWebsocketManager
import logging
import os # To potentially read keys from env vars

# Configure logging to see potential errors from the library
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
# Public streams like kline or miniTicker usually don't need API keys
# API_KEY = os.environ.get('BINANCE_API_KEY') or "YOUR_API_KEY_IF_NEEDED"
# API_SECRET = os.environ.get('BINANCE_SECRET_KEY') or "YOUR_SECRET_KEY_IF_NEEDED"
API_KEY = None # Set to None for public streams
API_SECRET = None

# Stream to test (choose one):
# stream_type = "kline" # Less frequent updates
stream_type = "miniTicker" # More frequent updates
# stream_type = "aggTrade" # Very frequent updates

symbol = 'BTCUSDT' # Symbol for kline stream
interval = '1m' # Interval for kline stream

# --- WebSocket Handling ---

def handle_message(msg):
    """Simple callback to print received messages."""
    print(f"Received message: {msg}")

# --- Main Execution ---
if __name__ == "__main__":

    print("--- WebSocket Test Script ---")
    print(f"Attempting to connect to {stream_type} stream...")

    twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)
    twm.start() # Start the manager thread

    stream_name = None
    try:
        if stream_type == "kline":
            print(f"Starting kline stream: {symbol.lower()}@kline_{interval}")
            # Note: Method is start_symbol_kline_socket, not start_kline_socket
            stream_name = twm.start_symbol_kline_socket(callback=handle_message, symbol=symbol, interval=interval)
        elif stream_type == "miniTicker":
            # Use multiplex socket for !miniTicker@arr (all symbols)
            stream_to_start = '!miniTicker@arr'
            print(f"Starting multiplex stream: {stream_to_start}")
            stream_name = twm.start_multiplex_socket(callback=handle_message, streams=[stream_to_start])
        elif stream_type == "aggTrade":
            # Use multiplex socket for !aggTrade@arr (all symbols)
            stream_to_start = '!aggTrade@arr'
            print(f"Starting multiplex stream: {stream_to_start}")
            stream_name = twm.start_multiplex_socket(callback=handle_message, streams=[stream_to_start])
        else:
            print(f"Error: Unknown stream_type '{stream_type}'")
            twm.stop()
            exit()

        if stream_name:
            print(f"Stream started with name: {stream_name}")
            print("Waiting for messages... Press CTRL+C to stop.")
            # Keep the main thread alive
            twm.join()
        else:
             print("Failed to start stream.")

    except Exception as e:
        print(f"An error occurred during stream setup or execution: {e}")
    finally:
        print("\nStopping WebSocket Manager...")
        twm.stop()
        print("Script finished.") 