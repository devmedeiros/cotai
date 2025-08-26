import logging
import os
from datetime import datetime

def setup_logger(name, log_file=None, level=logging.INFO):
    """
    Configura e retorna um logger personalizado
    
    Args:
        name: Nome do logger (geralmente __name__)
        log_file: Nome do arquivo de log (opcional)
        level: Nível de logging (default: INFO)
    
    Returns:
        Logger configurado
    """
    
    # Criar diretório de logs se não existir
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    
    # Se não especificou arquivo, usa o nome do módulo
    if not log_file:
        log_file = f"{name.split('.')[-1]}.log"
    
    log_path = os.path.join(log_dir, log_file)
    
    # Configurar formato
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Criar logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Evitar duplicação de handlers
    if logger.handlers:
        logger.handlers.clear()
    
    # Handler para arquivo
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Handler para console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger