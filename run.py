# -*- coding: utf-8 -*-
# Import both app and socketio instance from the app package
from app import app, socketio
from app.services import websocket_streams # Import the new service
import atexit # To register cleanup function
import logging
from app import create_app
# Importar funções WebSocket renomeadas
from app.services.binance_service import start_websocket_listeners, stop_websocket_listeners
import threading

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = create_app()

# --- Configuração do Listener WebSocket --- #
SYMBOLS_TO_MONITOR_AGGTRADE = ['BTCUSDT'] # Lista de símbolos apenas para aggTrade
websocket_thread = None

def run_websocket():
    """Função para rodar o listener em uma thread."""
    try:
        # Passa a lista de símbolos para aggTrade
        start_websocket_listeners(SYMBOLS_TO_MONITOR_AGGTRADE)
    except Exception as e:
        logging.error(f"Erro fatal na thread do WebSocket: {e}")

if __name__ == '__main__':
    logging.info("Iniciando aplicação Crypto Monitor...")

    # Initialize data caches and start Binance WebSocket manager
    # OBS: A linha abaixo pode ser redundante ou conflitar com nosso listener iniciado aqui.
    # Vamos comentar por enquanto, pois estamos iniciando nosso próprio listener.
    # websocket_streams.initialize_websocket_manager()

    # Registrar a função de parada para ser chamada ao sair
    atexit.register(stop_websocket_listeners) # Usa a função de parada renomeada
    # A linha abaixo parece ser de uma estrutura antiga, comentar também:
    # atexit.register(websocket_streams.stop_websocket_manager)

    # Iniciar o listener WebSocket em uma thread separada
    logging.info(f"Iniciando listener WebSocket para aggTrade({SYMBOLS_TO_MONITOR_AGGTRADE}) e Liquidações em background...")
    websocket_thread = threading.Thread(target=run_websocket, daemon=True) # daemon=True permite que o programa saia mesmo se a thread estiver rodando
    websocket_thread.start()

    # Run the Flask app using SocketIO server
    print("Starting Flask-SocketIO server with eventlet...")
    # Nota: Se você não estiver usando Flask-SocketIO ativamente ainda, pode comentar 
    # a linha socketio.run e descomentar a linha app.run abaixo.
    socketio.run(app,
                 host='0.0.0.0', # Ou 127.0.0.1
                 port=5000,
                 use_reloader=False, # Importante manter False
                 debug=False) # Debug False recomendado
    
    # Linha original do app.run (manter comentada se usar socketio.run)
    # app.run(host='127.0.0.1', port=5000, debug=False, use_debugger=False, use_reloader=False)

    logging.info("Aplicação Crypto Monitor encerrada.") 