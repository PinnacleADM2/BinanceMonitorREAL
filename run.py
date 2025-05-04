# -*- coding: utf-8 -*-
# Import both app and socketio instance from the app package
from app import app, socketio
from app.services import websocket_streams # Import the new service
import atexit # To register cleanup function
import logging
from app import create_app
# Importar funções WebSocket renomeadas
from app.services.binance_service import (
    start_spot_kline_listeners, stop_spot_kline_listeners, # Para Spot
    start_futures_listeners, stop_futures_listeners  # Para Futures
)
import threading

# Importar símbolos da config
try:
    from config import FUTURES_SYMBOLS_AGGTRADE, SPOT_SYMBOLS_KLINES
except ImportError:
    logging.error("Não foi possível importar listas de símbolos de config.py. Usando listas vazias.")
    FUTURES_SYMBOLS_AGGTRADE = []
    SPOT_SYMBOLS_KLINES = []

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = create_app()

# --- Configuração do Listener WebSocket --- #
SYMBOLS_TO_MONITOR_AGGTRADE = ['BTCUSDT'] # Lista de símbolos apenas para aggTrade
websocket_thread = None

# --- Threads WebSocket --- #
spot_websocket_thread = None
futures_websocket_thread = None

def run_spot_websocket():
    """Função para rodar o listener Spot Kline em uma thread."""
    logging.info(f"Thread Spot iniciando listener para {len(SPOT_SYMBOLS_KLINES)} símbolos...")
    try:
        start_spot_kline_listeners(SPOT_SYMBOLS_KLINES)
    except Exception as e:
        logging.error(f"Erro fatal na thread do WebSocket Spot: {e}", exc_info=True)

def run_futures_websocket():
    """Função para rodar o listener Futures (AggTrade+Liq) em uma thread."""
    logging.info(f"Thread Futures iniciando listener para {len(FUTURES_SYMBOLS_AGGTRADE)} aggTrades + Liquidações...")
    try:
        start_futures_listeners(FUTURES_SYMBOLS_AGGTRADE)
    except Exception as e:
        logging.error(f"Erro fatal na thread do WebSocket Futures: {e}", exc_info=True)

if __name__ == '__main__':
    logging.info("Iniciando aplicação Crypto Monitor...")

    # Initialize data caches and start Binance WebSocket manager
    # OBS: A linha abaixo pode ser redundante ou conflitar com nosso listener iniciado aqui.
    # Vamos comentar por enquanto, pois estamos iniciando nosso próprio listener.
    # websocket_streams.initialize_websocket_manager()

    # Registrar funções de parada para serem chamadas ao sair
    atexit.register(stop_spot_kline_listeners)
    atexit.register(stop_futures_listeners)
    # Remover registros antigos se houver
    # atexit.unregister(...) # Se necessário

    # --- Iniciar Listener SPOT --- #
    if SPOT_SYMBOLS_KLINES:
        logging.info(f"Iniciando listener WebSocket SPOT Klines em background...")
        spot_websocket_thread = threading.Thread(target=run_spot_websocket, name="SpotKlineThread", daemon=True)
        spot_websocket_thread.start()
    else:
        logging.warning("Nenhum símbolo SPOT definido em config.py, listener de Klines não iniciado.")

    # --- Iniciar Listener FUTURES --- #
    if FUTURES_SYMBOLS_AGGTRADE: # Inicia mesmo se a lista estiver vazia para pegar liquidações
        logging.info(f"Iniciando listener WebSocket FUTURES (AggTrade+Liquidation) em background...")
        futures_websocket_thread = threading.Thread(target=run_futures_websocket, name="FuturesThread", daemon=True)
        futures_websocket_thread.start()
    # else: # Poderia logar se nem liquidações fossem desejadas
        # logging.warning("Nenhum símbolo FUTURES definido em config.py, listener de AggTrade não iniciado (apenas Liquidação global).")

    # --- Iniciar Servidor Flask/SocketIO --- #
    logging.info("Iniciando servidor Flask-SocketIO...")
    try:
        socketio.run(app,
                     host='0.0.0.0', # Ou 127.0.0.1
                     port=5000,
                     use_reloader=False,
                     debug=False, # Manter False em produção/teste com threads
                     log_output=True) # Mostrar logs do SocketIO/EngineIO
    except Exception as e:
        logging.error(f"Erro ao iniciar servidor Flask-SocketIO: {e}", exc_info=True)
    finally:
        logging.info("Aplicação Crypto Monitor encerrada ou encontrou um erro fatal.")
        # Chamadas atexit cuidarão da limpeza dos listeners

    logging.info("Aplicação Crypto Monitor encerrada.") 