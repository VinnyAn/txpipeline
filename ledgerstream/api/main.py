import logging
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
from db.models import TransactionRaw
from db.session import SessionLocal
from api.schemas import TransactionIn, TransactionOut

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)
 
# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
 
# O FastAPI cria a aplicação. O título e a versão aparecem na documentação
# automática que o FastAPI gera em /docs
app = FastAPI(
    title="LedgerStream API",
    version="0.1.0",
    description="API de ingestão de transações financeiras",
)
 
# ---------------------------------------------------------------------------
# Dependency — conexão com o banco
# ---------------------------------------------------------------------------
 
def get_db():
    """
    Essa função é uma 'dependency' do FastAPI.
    Ela abre uma conexão com o banco para cada requisição
    e fecha automaticamente quando a requisição termina —
    mesmo que dê erro no meio do caminho.
    """
    db = SessionLocal()
    try:
        yield db          # entrega a conexão para o endpoint
    finally:
        db.close()        # sempre fecha, com ou sem erro
 
# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
 
@app.get("/health")
def health():
    """
    Endpoint de health check.
    O Kubernetes vai bater aqui para saber se a API está viva.
    Retorna 200 OK se estiver no ar.
    """
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}
 
 
@app.post("/transactions", response_model=TransactionOut, status_code=201)
def create_transaction(
    payload: TransactionIn,       # FastAPI valida automaticamente via Pydantic
    db: Session = Depends(get_db) # FastAPI injeta a conexão com o banco
):
    """
    Recebe uma transação, valida e persiste no PostgreSQL.
 
    - 201 Created  → transação salva com sucesso
    - 409 Conflict → transação já existe (mesmo transaction_id)
    - 422 Unprocessable → dados inválidos (Pydantic rejeitou)
    """
 
    # Verifica se a transação já foi processada (idempotência)
    # Isso evita duplicatas se o consumer reenviar a mesma mensagem
    existing = db.query(TransactionRaw).filter(
        TransactionRaw.transaction_id == payload.transaction_id
    ).first()
 
    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"Transação {payload.transaction_id} já foi processada"
        )
 
    # Cria o objeto do banco a partir dos dados validados pelo Pydantic
    transaction = TransactionRaw(
        transaction_id=payload.transaction_id,
        user_id=payload.user_id,
        amount=payload.amount,
        currency=payload.currency,
        transaction_type=payload.transaction_type,
        status=payload.status,
        merchant_name=payload.merchant_name,
        merchant_category=payload.merchant_category,
        card_last4=payload.card_last4,
        latitude=payload.latitude,
        longitude=payload.longitude,
        created_at=payload.created_at,
        ingested_at=datetime.now(timezone.utc),
    )
 
    # Salva no banco
    db.add(transaction)
    db.commit()
    db.refresh(transaction)  # atualiza o objeto com os dados do banco (ex: ingested_at)
 
    logger.info("Transação %s salva com status=%s", transaction.transaction_id, transaction.status)
 