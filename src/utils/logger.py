import logging
from pathlib import Path

Path("logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ]
)

def get_logger(name: str) -> logging.Logger:
    """
    Devuelve un logger configurado para el módulo que lo solicita.
    """
    return logging.getLogger(name)