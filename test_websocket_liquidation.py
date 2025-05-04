# -*- coding: utf-8 -*-
import time
from binance import ThreadedWebsocketManager
import logging
import datetime
from collections import deque, defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_KEY = None
API_SECRET = None

# --- Constantes e Estruturas para Teste --- #
LOOKBACK_SECONDS = 60
THRESHOLDS_USDT_LIQUIDATION = {
    "50k": 50_000,
    "10k": 10_000
}
ORDERED_THRESHOLDS_LIQUIDATION = sorted(THRESHOLDS_USDT_LIQUIDATION.items(), key=lambda item: item[1], reverse=True)
recent_large_liquidations_test = defaultdict(lambda: deque(maxlen=5000)) # Deque local para o teste

def handle_message(msg):
    # print(f"LIQUIDATION Raw Received: {msg}") # Linha original comentada

    if not isinstance(msg, dict) or 'stream' not in msg or msg['stream'] != '!forceOrder@arr' or 'data' not in msg:
        # logging.debug(f"Mensagem não esperada no stream de liquidação: {msg}")
        return
    
    msg_data = msg['data']
    if not isinstance(msg_data, dict) or 'o' not in msg_data:
         logging.warning(f"Formato inesperado nos dados de liquidação: {msg_data}")
         return

    order_data = msg_data.get('o', {})
    symbol = order_data.get('s')

    try:
        liq_quantity = float(order_data.get('q', 0))
        avg_price = float(order_data.get('p', 0))
        liq_time_ms = int(order_data.get('T', 0))
        side = order_data.get('S', None)

        if symbol is None or liq_time_ms == 0 or side is None:
            # logging.warning(f"Mensagem de liquidação incompleta recebida: {order_data}")
            return

        liq_value_usdt = liq_quantity * avg_price
        alert_triggered_test = False

        # Verifica os thresholds de liquidação do maior para o menor
        for level_name, threshold_value in ORDERED_THRESHOLDS_LIQUIDATION:
            if liq_value_usdt >= threshold_value:
                alert_triggered_test = True
                # --- Cálculo do Lookback (simplificado para teste, sem persistência real entre runs) --- #
                one_minute_ago_ms = liq_time_ms - (LOOKBACK_SECONDS * 1000)
                lookback_count_at_level = 0
                liquidations_in_window = list(recent_large_liquidations_test[symbol])

                for past_ts, past_value, past_side in liquidations_in_window:
                    if past_ts >= one_minute_ago_ms and past_ts < liq_time_ms:
                        if past_value >= threshold_value:
                             if past_side == side:
                                lookback_count_at_level += 1
                # --- Fim Lookback --- #

                # Formata e imprime o alerta
                liq_time_dt = datetime.datetime.fromtimestamp(liq_time_ms / 1000, tz=datetime.timezone.utc)
                base_currency = symbol[:-4] if symbol.endswith('USDT') else 'Tokens'
                alert_msg = (
                    f"\n*** TESTE: ALERTA LIQUIDAÇÃO (NÍVEL > {level_name} USDT) ***\n" +
                    f"  Ativo: {symbol}\n" +
                    f"  Valor Liquidado: {liq_value_usdt:,.2f} USDT ({liq_quantity:.4f} {base_currency})\n" +
                    f"  Preço Médio: {avg_price:.8f} USDT\n" +
                    f"  Lado: {side}\n" +
                    f"  Hora: {liq_time_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} UTC\n" +
                    f"  Liq. >= {level_name} USDT (mesmo lado/ativo) no último min: {lookback_count_at_level}\n" +
                    f"*******************************************************"
                )
                logging.warning(alert_msg) # Usar warning para destacar
                break # Processa apenas o maior nível
        
        # Adiciona ao deque de teste se atingiu o menor limiar
        lowest_liq_threshold = ORDERED_THRESHOLDS_LIQUIDATION[-1][1]
        if liq_value_usdt >= lowest_liq_threshold:
            one_minute_ago_ms_for_pruning = time.time() * 1000 - (LOOKBACK_SECONDS * 1000 * 1.5)
            while recent_large_liquidations_test[symbol] and recent_large_liquidations_test[symbol][0][0] < one_minute_ago_ms_for_pruning:
                recent_large_liquidations_test[symbol].popleft()
            recent_large_liquidations_test[symbol].append((liq_time_ms, liq_value_usdt, side))
        
        # Opcional: Informar se recebeu liquidação abaixo do limiar mínimo
        # elif not alert_triggered_test:
        #      logging.info(f"Liquidação recebida para {symbol} abaixo do limiar ({liq_value_usdt:,.2f} USDT).")

    except (ValueError, TypeError) as e:
        logging.error(f"Erro ao processar dados de liquidação {order_data}: {e}")
    except Exception as e:
        logging.error(f"Erro inesperado no handle_message para {symbol}: {e}", exc_info=True)

if __name__ == "__main__":
    print("--- WebSocket Test Script: Liquidation Orders (com cálculo USDT) ---")
    stream_to_start = '!forceOrder@arr'
    print(f"Attempting to connect to {stream_to_start}...")

    twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)
    twm.start()

    stream_name = None
    try:
        stream_name = twm.start_multiplex_socket(callback=handle_message, streams=[stream_to_start])
        if stream_name:
            print(f"Stream started: {stream_name}")
            print("Waiting for liquidation messages... Press CTRL+C to stop.")
            twm.join()
        else:
            print("Failed to start stream.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("\nStopping WebSocket Manager...")
        if twm:
            twm.stop()
        print("Script finished.") 