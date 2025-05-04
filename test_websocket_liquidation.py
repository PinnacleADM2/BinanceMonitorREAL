# -*- coding: utf-8 -*-
import time
from binance import ThreadedWebsocketManager
import logging
import datetime
from collections import deque, defaultdict
from decimal import Decimal, InvalidOperation

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_KEY = None
API_SECRET = None

# --- Constantes e Estruturas para Teste --- #
LOOKBACK_SECONDS = 60
THRESHOLDS_USDT_LIQUIDATION = {
    "50k": Decimal("50000"),
    "10k": Decimal("10000")
}
ORDERED_THRESHOLDS_LIQUIDATION = sorted(THRESHOLDS_USDT_LIQUIDATION.items(), key=lambda item: item[1], reverse=True)
recent_large_liquidations_test = defaultdict(lambda: deque(maxlen=5000)) # Deque local para o teste

def _safe_decimal_test(value):
    if value is None: return None
    try:
        return Decimal(value)
    except InvalidOperation:
        logging.error(f"[TEST] Falha ao converter valor para Decimal: '{value}'")
        return None

def handle_message(msg):
    # print(f"LIQUIDATION Raw Received: {msg}") # Linha original comentada

    if not isinstance(msg, dict) or 'stream' not in msg or msg['stream'] != '!forceOrder@arr' or 'data' not in msg:
        # logging.debug(f"Mensagem não esperada no stream de liquidação: {msg}")
        return
    
    msg_data = msg['data']
    if not isinstance(msg_data, dict) or 'o' not in msg_data:
         logging.warning(f"[TEST] Formato inesperado nos dados de liquidação: {msg_data}")
         return

    order_data = msg_data.get('o', {})
    symbol = order_data.get('s')

    try:
        # Usar Decimal para conversão
        liq_quantity_decimal = _safe_decimal_test(order_data.get('q'))
        avg_price_decimal = _safe_decimal_test(order_data.get('p'))
        liq_time_ms = int(order_data.get('T', 0))
        side = order_data.get('S', None)

        if None in (symbol, liq_quantity_decimal, avg_price_decimal, side) or liq_time_ms == 0:
            # logging.warning(f"Mensagem de liquidação incompleta recebida: {order_data}")
            return

        liq_value_usdt_decimal = liq_quantity_decimal * avg_price_decimal
        alert_triggered_test = False

        # Verifica os thresholds de liquidação do maior para o menor
        for level_name, threshold_value in ORDERED_THRESHOLDS_LIQUIDATION:
            if liq_value_usdt_decimal >= threshold_value:
                alert_triggered_test = True
                # --- Cálculo do Lookback (simplificado para teste, sem persistência real entre runs) --- #
                one_minute_ago_ms = liq_time_ms - (LOOKBACK_SECONDS * 1000)
                lookback_count_at_level = 0
                liquidations_in_window = list(recent_large_liquidations_test[symbol])

                for past_ts, past_value, past_side in liquidations_in_window:
                    if past_ts >= one_minute_ago_ms and past_ts < liq_time_ms:
                        # Compara com Decimal
                        if past_value >= threshold_value:
                             if past_side == side:
                                lookback_count_at_level += 1
                # --- Fim Lookback --- #

                # Formata e imprime o alerta
                liq_time_dt = datetime.datetime.fromtimestamp(liq_time_ms / 1000, tz=datetime.timezone.utc)
                base_currency = symbol[:-4] if symbol.endswith('USDT') else 'Tokens'
                alert_msg = (
                    f"\n*** TESTE: ALERTA LIQUIDAÇÃO (NÍVEL > {level_name} USDT) ***\n" +
                    # Usar float para formatação de string, mas Decimal para cálculo
                    f"  Ativo: {symbol}\n" +
                    f"  Valor Liquidado: {float(liq_value_usdt_decimal):,.2f} USDT ({float(liq_quantity_decimal):.4f} {base_currency})\n" +
                    f"  Preço Médio: {float(avg_price_decimal):.8f} USDT\n" +
                    f"  Lado: {side}\n" +
                    f"  Hora: {liq_time_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} UTC\n" +
                    f"  Liq. >= {level_name} USDT (mesmo lado/ativo) no último min: {lookback_count_at_level}\n" +
                    f"*******************************************************"
                )
                logging.warning(alert_msg) # Usar warning para destacar
                break # Processa apenas o maior nível
        
        # Adiciona ao deque de teste se atingiu o menor limiar
        lowest_liq_threshold = ORDERED_THRESHOLDS_LIQUIDATION[-1][1]
        if liq_value_usdt_decimal >= lowest_liq_threshold:
            one_minute_ago_ms_for_pruning = time.time() * 1000 - (LOOKBACK_SECONDS * 1000 * 1.5)
            while recent_large_liquidations_test[symbol] and recent_large_liquidations_test[symbol][0][0] < one_minute_ago_ms_for_pruning:
                recent_large_liquidations_test[symbol].popleft()
            # Armazena Decimal no deque
            recent_large_liquidations_test[symbol].append((liq_time_ms, liq_value_usdt_decimal, side))
        
        # Opcional: Informar se recebeu liquidação abaixo do limiar mínimo
        # elif not alert_triggered_test:
        #      logging.info(f"Liquidação recebida para {symbol} abaixo do limiar ({liq_value_usdt_decimal:,.2f} USDT).")

    except (ValueError, TypeError) as e:
        logging.error(f"[TEST] Erro ao processar dados de liquidação {order_data}: {e}")
    except Exception as e:
        logging.error(f"[TEST] Erro inesperado no handle_message para {symbol}: {e}", exc_info=True)

if __name__ == "__main__":
    print("--- WebSocket Test Script: Liquidation Orders (FUTURES Endpoint com cálculo USDT/Decimal) ---")
    stream_to_listen = '!forceOrder@arr'
    print(f"Attempting to connect to Futures stream: {stream_to_listen}...")

    # Usar um novo TWM apenas para este teste pode ser mais limpo
    twm_test = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)
    twm_test.start()

    stream_name = None
    try:
        # Conectar usando is_futures=True # <<< REMOVIDO
        # stream_name = twm_test.start_multiplex_socket(
        #     callback=handle_message,
        #     streams=[stream_to_listen],
        #     is_futures=True # *** Essencial para conectar ao endpoint correto! ***
        # )

        # Alternativa (se is_futures não funcionar ou versão < 1.0.18): # <<< ATIVADO
        stream_name = twm_test.start_custom_socket(
            callback=handle_message,
            url=f"wss://fstream.binance.com/ws/{stream_to_listen}"
        )

        if stream_name:
            print(f"Stream started on Futures endpoint (Custom Socket ID): {stream_name}")
            print("Waiting for liquidation messages... Press CTRL+C to stop.")
            twm_test.join()
        else:
            print("Failed to start Futures stream.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("\nStopping Test WebSocket Manager...")
        if twm_test:
            twm_test.stop()
        print("Test script finished.") 