import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from extract.extract_raw import main as extract_main
from transform.transform_silver import main as silver_main
from load.transform_gold import main as gold_main
from enrich.summarize import main as enrich_main
from utils.logger import setup_logger

logger = setup_logger(__name__) 

def run_pipeline():
    logger.info("Iniciando pipeline completo")
    
    logger.info("Executando extração")
    extract_main()

    logger.info("Executando transformação silver")
    silver_main()

    logger.info("Executando transformação e load gold")
    gold_main()

    logger.info("Gerando insight diário")
    enrich_main()
    
    logger.info("Pipeline concluído")

if __name__ == "__main__":
    run_pipeline()