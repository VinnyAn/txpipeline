#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$ROOT_DIR/.venv"
APP="api.main:app"
PORT=8000

echo "LedgerStream — iniciando API"
echo "Diretório raiz: $ROOT_DIR"

if [ -d "$VENV_DIR" ]; then
    echo "virtualenv encontrado, ativando..."
    source "$VENV_DIR/bin/activate"
else
    echo "virtualenv não encontrado, criando..."
    python3 -m venv "$VENV_DIR"
    source "$VENV_DIR/bin/activate"

    if [ -f "$ROOT_DIR/requirements.txt" ]; then
        echo "Instalando dependências do requirements.txt..."
        pip install --upgrade pip -q
        pip install -r "$ROOT_DIR/requirements.txt"
    else
        echo "Aviso: requirements.txt não encontrado, pulando instalação."
    fi
fi

echo "Python: $(which python)"
echo "Iniciando FastAPI na porta $PORT..."
cd "$ROOT_DIR"
uvicorn $APP --reload --host 0.0.0.0 --port $PORT