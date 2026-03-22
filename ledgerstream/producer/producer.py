import json
import random
import time
import logging
from datetime import datetime, timezone
from uuid import uuid4

from faker import Faker
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    )

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
TOPIC = 'transactions-raw'

TRANSACTION_TYPES = ["purchase", "refund", "transfer", "withdrawal", "deposit"]
CURRENCIES = ["BRL", "USD", "EUR"]
STATUSES = ["approved", "pending", "declined"]
STATUS_WEIGHTS = [0.75, 0.15, 0.10]

TRANSACTION_SCHEMA = json.dumps({
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Transaction",
    "type": "object",
    "properties": {
        "transaction_id":   {"type": "string"},
        "user_id":          {"type": "string"},
        "amount":           {"type": "number"},
        "currency":         {"type": "string"},
        "transaction_type": {"type": "string"},
        "status":           {"type": "string"},
        "merchant_name":    {"type": "string"},
        "merchant_category":{"type": "string"},
        "card_last4":       {"type": "string"},
        "latitude":         {"type": "number"},
        "longitude":        {"type": "number"},
        "created_at":       {"type": "string", "format": "date-time"},
    },
    "required": [
        "transaction_id", "user_id", "amount", "currency",
        "transaction_type", "status", "created_at",
    ],
})

fake = Faker("pt_BR")

def generate_transaction() -> dict:
    """ Gera uma transação aleatória e realista. """
    status = random.choices(STATUSES, weights=STATUS_WEIGHTS)[0]

    amount = round(random.uniform(5.0, 5000.0), 2)
    if amount == "refund":
        amount = -amount

    lat, lon, _, _, _ = fake.local_latlng(country_code="BR")

    return {
        "transaction_id":    str(uuid4()),
        "user_id":           str(uuid4()),
        "amount":            amount,
        "currency":          random.choice(CURRENCIES),
        "transaction_type":  random.choice(TRANSACTION_TYPES),
        "status":            status,
        "merchant_name":     fake.company(),
        "merchant_category": fake.bs().split()[0].capitalize(),
        "card_last4":        fake.credit_card_number()[-4:],
        "latitude":          float(lat),
        "longitude":         float(lon),
        "created_at":        datetime.now(timezone.utc).isoformat(),
    }

def delivery_report(err, msg):
    if err:
        logger.error("Falha ao entregar mensagem: %s", err)
    else:
        logger.info(
            "Mensagem entregue → tópico=%s partição=%d offset=%d",
            msg.topic(), msg.partition(), msg.offset(),
        )

def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        # Garante que a mensagem só é confirmada quando todas as réplicas receberam
        "acks": "all",
        # Agrupa mensagens em lotes de até 16 KB antes de enviar
        "linger.ms": 10,
        "compression.type": "snappy",
        "retries": 5,
        "retry.backoff.ms": 300,
    })
 
 
def build_serializer() -> JSONSerializer:
    """Cria um serializer integrado ao Schema Registry."""
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    return JSONSerializer(
        TRANSACTION_SCHEMA,
        schema_registry_client,
        # Garante que o schema está registrado antes de publicar
        conf={"auto.register.schemas": True},
    )
 
 
def run(
    batch_size: int = 10,
    interval_seconds: float = 1.0,
    max_batches: int | None = None,
) -> None:
    """
    Publica transações em lotes contínuos.
 
    Args:
        batch_size:       Mensagens por lote.
        interval_seconds: Pausa entre lotes.
        max_batches:      None = roda para sempre; int = para após N lotes.
    """
    producer = build_producer()
    serializer = build_serializer()
    batches_sent = 0
 
    logger.info(
        "Iniciando producer → tópico=%s batch=%d intervalo=%.1fs",
        TOPIC, batch_size, interval_seconds,
    )
 
    try:
        while max_batches is None or batches_sent < max_batches:
            for _ in range(batch_size):
                transaction = generate_transaction()
 
                producer.produce(
                    topic=TOPIC,
                    # Usar o user_id como key garante que transações do mesmo
                    # usuário sempre caiam na mesma partição (ordenação garantida)
                    key=transaction["user_id"],
                    value=serializer(
                        transaction,
                        SerializationContext(TOPIC, MessageField.VALUE),
                    ),
                    on_delivery=delivery_report,
                )
 
            # Força o envio do buffer antes de dormir
            producer.flush()
            batches_sent += 1
            logger.info("Lote %d enviado (%d mensagens)", batches_sent, batch_size)
            time.sleep(interval_seconds)
 
    except KeyboardInterrupt:
        logger.info("Interrompido pelo usuário.")
    finally:
        producer.flush()
        logger.info("Producer finalizado. Total de lotes: %d", batches_sent)

if __name__ == "__main__":
    run(
        batch_size=10,
        interval_seconds=1.0,
        max_batches=None,           # Troque por um número para testes pontuais
    )