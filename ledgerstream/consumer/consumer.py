import json
import logging
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from core.config import Loadenv

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
env = Loadenv.from_env()

CONSUMER_GROUP    = "transactions-ingestion-group"
API_URL           = env.API_URL
MAX_RETRIES       = 3
RETRY_BACKOFF     = 2.0   # segundos entre tentativas

# ---------------------------------------------------------------------------
# Autenticação JWT
# ---------------------------------------------------------------------------

def get_jwt_token() -> str:
    """
    Autentica na FastAPI e retorna o token JWT.
    Chamado uma vez ao iniciar e novamente se o token expirar.
    """
    logger.info("Obtendo token JWT...")
    response = requests.post(
        f"{API_URL}/auth/token",
        data={
            "username": "consumer",
            "password": "consumer123",
        },
        timeout=10,
    )
    response.raise_for_status()
    token = response.json()["access_token"]
    logger.info("Token JWT obtido com sucesso")
    return token

# ---------------------------------------------------------------------------
# Dead Letter Queue — envia mensagens com erro para o DLQ
# ---------------------------------------------------------------------------

def send_to_dlq(producer: Producer, message_value: bytes, reason: str) -> None:
    """
    Envia a mensagem original para o tópico DLQ junto com o motivo do erro.
    """
    dlq_payload = {
        "original_message": message_value.decode("utf-8") if message_value else None,
        "error_reason":     reason,
        "failed_at":        datetime.now(timezone.utc).isoformat(),
    }
    producer.produce(
        topic="transactions-dlq",
        value=json.dumps(dlq_payload).encode("utf-8"),
    )
    producer.flush()
    logger.warning("Mensagem enviada para DLQ: %s", reason)

# ---------------------------------------------------------------------------
# Envio para a FastAPI com retry
# ---------------------------------------------------------------------------

def send_to_api(transaction: dict, token: str) -> tuple[bool, bool]:
    """
    Envia a transação para a FastAPI.

    Retorna:
        (sucesso, token_expirado)
        - sucesso=True  → commit o offset
        - sucesso=False → tentar novamente ou DLQ
        - token_expirado=True → buscar novo token e tentar novamente
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(
                f"{API_URL}/transactions",
                json=transaction,
                headers=headers,
                timeout=10,
            )

            # Sucesso
            if response.status_code == 201:
                return True, False

            # Transação já existia — não é erro, faz o commit normalmente
            if response.status_code == 409:
                logger.warning("Transação duplicada, ignorando: %s", transaction.get("transaction_id"))
                return True, False

            # Token expirado — buscar novo token
            if response.status_code == 401:
                logger.warning("Token JWT expirado, renovando...")
                return False, True

            # Erro de validação — não adianta tentar de novo
            if response.status_code == 422:
                logger.error("Dados inválidos: %s", response.json())
                return False, False

            logger.warning(
                "Tentativa %d/%d falhou: status=%d",
                attempt, MAX_RETRIES, response.status_code,
            )

        except requests.exceptions.ConnectionError:
            logger.warning("Tentativa %d/%d — API indisponível", attempt, MAX_RETRIES)

        except requests.exceptions.Timeout:
            logger.warning("Tentativa %d/%d — timeout", attempt, MAX_RETRIES)

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF * attempt)

    return False, False

# ---------------------------------------------------------------------------
# Consumer principal
# ---------------------------------------------------------------------------

def build_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers":        env.KAFKA_BOOTSTRAP_SERVERS,
        "group.id":                 CONSUMER_GROUP,
        # Commitamos o offset manualmente após confirmar que a API salvou
        "enable.auto.commit":       False,
        # Se não houver offset salvo, começa do início
        "auto.offset.reset":        "earliest",
        "session.timeout.ms":       30000,
        "max.poll.interval.ms":     300000,
    })


def build_dlq_producer() -> Producer:
    return Producer({"bootstrap.servers": env.KAFKA_BOOTSTRAP_SERVERS})


def run() -> None:
    consumer    = build_consumer()
    dlq_producer = build_dlq_producer()
    token       = get_jwt_token()

    consumer.subscribe([env.KAFKA_TOPIC])
    logger.info("Consumer iniciado → grupo=%s tópico=%s", CONSUMER_GROUP, env.KAFKA_TOPIC)

    try:
        while True:
            # Aguarda até 1 segundo por uma mensagem
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Chegou ao fim da partição — normal, continua aguardando
                    continue
                raise KafkaException(msg.error())

            # Desserializa o JSON da mensagem
            try:
                transaction = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                logger.error("Erro ao desserializar mensagem: %s", e)
                send_to_dlq(dlq_producer, msg.value(), f"Desserialização falhou: {e}")
                consumer.commit(message=msg)
                continue

            logger.info(
                "Mensagem recebida → partição=%d offset=%d id=%s",
                msg.partition(), msg.offset(), transaction.get("transaction_id"),
            )

            # Envia para a API com retry automático
            success, token_expired = send_to_api(transaction, token)

            # Token expirou — renova e tenta uma última vez
            if token_expired:
                token = get_jwt_token()
                success, _ = send_to_api(transaction, token)

            if success:
                # Confirma para o Kafka que a mensagem foi processada
                consumer.commit(message=msg)
                logger.info("Offset %d commitado", msg.offset())
            else:
                # Esgotou as tentativas — manda para o DLQ e avança
                send_to_dlq(dlq_producer, msg.value(), "Máximo de tentativas atingido")
                consumer.commit(message=msg)

    except KeyboardInterrupt:
        logger.info("Interrompido pelo usuário")
    finally:
        consumer.close()
        logger.info("Consumer finalizado")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run()
    