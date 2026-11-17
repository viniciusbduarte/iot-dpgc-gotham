#!/usr/bin/env python3
"""
Central de Inteligência de Gotham (DPGC) - Gateway IoT (Projeto 3)
Integra:
  1. Broker Mosquitto Local (127.0.0.1:1883)
  2. Banco de Dados de Série Temporal InfluxDB (DB: gotham_dpgc)
  3. Nuvem Adafruit IO via MQTT sobre TLS/SSL (io.adafruit.com:8883)
     - Envia telemetria (1 / 0) para o feed batsignal
     - Recebe Comando Reverso da Batcaverna e republica no Mosquitto local com retain=True
"""

import os
import ssl
import json
import time
import logging
from datetime import datetime, timezone
from influxdb import InfluxDBClient
import paho.mqtt.client as mqtt

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("GothamGateway")

# Configurações do Broker Local
LOCAL_MQTT_HOST = os.getenv("LOCAL_MQTT_HOST", "127.0.0.1")
LOCAL_MQTT_PORT = int(os.getenv("LOCAL_MQTT_PORT", "1883"))
TOPIC_BATSIGNAL = os.getenv("TOPIC_BATSIGNAL", "gotham/dpgc/vinicius/batsignal")
TOPIC_STATUS = os.getenv("TOPIC_STATUS", "gotham/dpgc/vinicius/status")

# Configurações do InfluxDB Local
INFLUX_HOST = os.getenv("INFLUX_HOST", "127.0.0.1")
INFLUX_PORT = int(os.getenv("INFLUX_PORT", "8086"))
INFLUX_DB = os.getenv("INFLUX_DB", "gotham_dpgc")

# Configurações da Nuvem (Adafruit IO MQTT TLS - WebSockets 443 ou TCP 8883)
AIO_HOST = os.getenv("AIO_HOST", "io.adafruit.com")
AIO_PORT = int(os.getenv("AIO_PORT", "443"))
AIO_TRANSPORT = os.getenv("AIO_TRANSPORT", "websockets").strip().lower()
AIO_WS_PATH = os.getenv("AIO_WS_PATH", "/mqtt").strip()
AIO_USERNAME = os.getenv("AIO_USERNAME", "viniciusduarte").strip()
AIO_KEY = os.getenv("AIO_KEY", "").strip()
AIO_FEED_BATSIGNAL = f"{AIO_USERNAME}/feeds/batsignal"

# Contadores de eco entre Nuvem <-> Local
pending_aio_echoes = 0
pending_local_echoes = 0
current_state_num = None

aio_client = None
local_client = None
influx_client = None


def parse_batsignal_payload(raw_payload: str):
    """
    Normaliza qualquer payload ('BAT_SIGNAL_ON', 'BAT_SIGNAL_OFF', '1', '0', 'ON', 'OFF')
    Retorna (state_str, state_num) ou (None, None) se inválido.
    """
    text = raw_payload.strip().upper()
    if text in ("BAT_SIGNAL_ON", "1", "ON", "TRUE"):
        return "BAT_SIGNAL_ON", 1
    if text in ("BAT_SIGNAL_OFF", "0", "OFF", "FALSE"):
        return "BAT_SIGNAL_OFF", 0
    return None, None


def write_batsignal_to_influx(state_str: str, state_num: int, source: str = "local"):
    """Grava o evento do Bat-Sinal no InfluxDB com timestamp UTC exato."""
    if influx_client is None:
        return
    now_iso = datetime.now(timezone.utc).isoformat()
    json_body = [
        {
            "measurement": "batsignal_events",
            "tags": {
                "topic": TOPIC_BATSIGNAL,
                "source": source,
            },
            "time": now_iso,
            "fields": {
                "state_str": state_str,
                "state_num": int(state_num),
            },
        }
    ]
    try:
        influx_client.write_points(json_body)
        logger.info(
            f"[InfluxDB] Gravado em batsignal_events: state_str={state_str}, state_num={state_num}, source={source}, time={now_iso}"
        )
    except Exception as exc:
        logger.error(f"[InfluxDB] Erro ao gravar batsignal_events: {exc}")


def write_status_to_influx(raw_payload: str):
    """Grava telemetria de keep-alive (gotham/dpgc/vinicius/status) no InfluxDB."""
    if influx_client is None:
        return
    try:
        data = json.loads(raw_payload)
        device = str(data.get("device", "unknown"))
        status = str(data.get("status", "ONLINE"))
        uptime_s = int(data.get("uptime_s", 0))
    except Exception:
        device = "unknown"
        status = raw_payload.strip()
        uptime_s = 0

    now_iso = datetime.now(timezone.utc).isoformat()
    json_body = [
        {
            "measurement": "device_status",
            "tags": {
                "topic": TOPIC_STATUS,
                "device": device,
            },
            "time": now_iso,
            "fields": {
                "status": status,
                "uptime_s": uptime_s,
            },
        }
    ]
    try:
        influx_client.write_points(json_body)
        logger.info(
            f"[InfluxDB] Gravado em device_status: device={device}, status={status}, uptime_s={uptime_s}"
        )
    except Exception as exc:
        logger.error(f"[InfluxDB] Erro ao gravar device_status: {exc}")


# =====================================================================
# Callbacks do Broker Local (Mosquitto no Raspberry Pi)
# =====================================================================
def on_local_connect(client, userdata, flags, rc, *args):
    if rc == 0:
        logger.info(f"[MQTT Local] Conectado em {LOCAL_MQTT_HOST}:{LOCAL_MQTT_PORT}")
        client.subscribe(TOPIC_BATSIGNAL, qos=1)
        client.subscribe(TOPIC_STATUS, qos=1)
        logger.info(f"[MQTT Local] Inscrito em '{TOPIC_BATSIGNAL}' e '{TOPIC_STATUS}'")
    else:
        logger.error(f"[MQTT Local] Falha na conexão, código: {rc}")


def on_local_message(client, userdata, msg):
    global pending_aio_echoes, pending_local_echoes, current_state_num
    payload = msg.payload.decode("utf-8", errors="replace").strip()
    logger.info(f"[MQTT Local] Mensagem recebida | Tópico: {msg.topic} | Payload: {payload} | Retain: {msg.retain}")

    if msg.topic == TOPIC_BATSIGNAL:
        state_str, state_num = parse_batsignal_payload(payload)
        if state_str is None:
            logger.warning(f"[MQTT Local] Payload desconhecido ignorado: {payload}")
            return

        current_state_num = state_num

        # Ignora a mensagem retida inicial ao reiniciar o serviço para não duplicar registros no banco
        if msg.retain:
            logger.info(f"[MQTT Local] Estado retido inicial carregado: {state_str} ({state_num})")
            return

        # Verifica se esta mensagem local foi gerada por um Comando Reverso da Nuvem
        if pending_local_echoes > 0:
            pending_local_echoes -= 1
            write_batsignal_to_influx(state_str, state_num, source="cloud")
            return

        # 1. Grava no InfluxDB como evento local (Parte A)
        write_batsignal_to_influx(state_str, state_num, source="local")

        # 2. Encaminha para o Adafruit IO via TLS se conectado (Parte B)
        if aio_client is not None and aio_client.is_connected():
            pending_aio_echoes += 1
            aio_client.publish(AIO_FEED_BATSIGNAL, str(state_num), qos=1, retain=False)
            logger.info(
                f"[Adafruit IO] Encaminhado via TLS ({AIO_PORT}) -> {AIO_FEED_BATSIGNAL}: {state_num} ({state_str})"
            )

    elif msg.topic == TOPIC_STATUS:
        write_status_to_influx(payload)


# =====================================================================
# Callbacks do Broker na Nuvem (Adafruit IO - TLS 8883)
# =====================================================================
def on_aio_connect(client, userdata, flags, rc, *args):
    if rc == 0:
        logger.info(f"[Adafruit IO] Conectado com TLS/SSL em {AIO_HOST}:{AIO_PORT}")
        client.subscribe(AIO_FEED_BATSIGNAL, qos=1)
        logger.info(f"[Adafruit IO] Inscrito no feed de comando reverso: {AIO_FEED_BATSIGNAL}")
    else:
        logger.error(f"[Adafruit IO] Falha na conexão TLS, código: {rc}")


def on_aio_message(client, userdata, msg):
    global pending_aio_echoes, pending_local_echoes, current_state_num
    payload = msg.payload.decode("utf-8", errors="replace").strip()
    logger.info(f"[Adafruit IO] Mensagem da Nuvem | Feed: {msg.topic} | Payload: {payload}")

    state_str, state_num = parse_batsignal_payload(payload)
    if state_str is None:
        logger.warning(f"[Adafruit IO] Payload inválido no feed: {payload}")
        return

    # Ignora o eco da mensagem que o próprio Gateway acabou de publicar na nuvem
    if pending_aio_echoes > 0:
        pending_aio_echoes -= 1
        return

    current_state_num = state_num
    logger.info(
        f"[Comando Reverso Batcaverna] Acionando Mosquitto Local -> {TOPIC_BATSIGNAL}: {state_str} (retain=True)"
    )
    if local_client is not None and local_client.is_connected():
        pending_local_echoes += 1
        local_client.publish(TOPIC_BATSIGNAL, state_str, qos=1, retain=True)


def main():
    global influx_client, local_client, aio_client

    # 1. Inicializa InfluxDB
    logger.info(f"Conectando ao InfluxDB em {INFLUX_HOST}:{INFLUX_PORT} (DB: {INFLUX_DB})...")
    influx_client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT)
    influx_client.create_database(INFLUX_DB)
    influx_client.switch_database(INFLUX_DB)
    logger.info("InfluxDB pronto!")

    # 2. Inicializa Cliente Adafruit IO (se AIO_KEY estiver configurada)
    if AIO_KEY:
        logger.info(
            f"Configurando ponte MQTT TLS ({AIO_TRANSPORT}:{AIO_PORT}) com Adafruit IO (usuário: {AIO_USERNAME})..."
        )
        try:
            aio_client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=f"gotham-gw-aio-{int(time.time())}",
                transport=AIO_TRANSPORT,
            )
        except AttributeError:
            aio_client = mqtt.Client(
                client_id=f"gotham-gw-aio-{int(time.time())}",
                transport=AIO_TRANSPORT,
            )

        if AIO_TRANSPORT == "websockets":
            aio_client.ws_set_options(path=AIO_WS_PATH)
        aio_client.username_pw_set(AIO_USERNAME, AIO_KEY)
        aio_client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
        aio_client.on_connect = on_aio_connect
        aio_client.on_message = on_aio_message
        aio_client.connect_async(AIO_HOST, AIO_PORT, keepalive=60)
        aio_client.loop_start()
    else:
        logger.warning(
            "AIO_KEY ainda não configurada. Rodando em modo Local (Mosquitto -> InfluxDB). "
            "Defina AIO_KEY no ambiente para ativar a nuvem Adafruit IO."
        )

    # 3. Inicializa Cliente Mosquitto Local
    try:
        local_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"gotham-gw-local-{int(time.time())}")
    except AttributeError:
        local_client = mqtt.Client(client_id=f"gotham-gw-local-{int(time.time())}")

    local_client.on_connect = on_local_connect
    local_client.on_message = on_local_message
    local_client.connect(LOCAL_MQTT_HOST, LOCAL_MQTT_PORT, keepalive=60)

    logger.info("Gateway da Central de Inteligência de Gotham em execução!")
    local_client.loop_forever()


if __name__ == "__main__":
    main()
