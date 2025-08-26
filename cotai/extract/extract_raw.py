import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import json
from datetime import date
from dotenv import load_dotenv
from utils.logger import setup_logger

logger = setup_logger(__name__)

def main():
    try:
        logger.info("Iniciando processo de extração de dados")

        load_dotenv()
        API_KEY = os.getenv('API_KEY')

        if not API_KEY:
            logger.error("API_KEY não encontrada nas variáveis de ambiente")
            raise ValueError("API_KEY não configurada")

        logger.info("API_KEY carregada com sucesso")

        today = str(date.today())
        logger.info(f"Data atual: {today}")

        BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        file_path = os.path.join(BASE_DIR, 'data', 'raw', f'{today}.json')

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        logger.info(f"Arquivo será salvo em: {file_path}")

        url = f'https://v6.exchangerate-api.com/v6/{API_KEY}/latest/BRL/'
        logger.info(f"Fazendo requisição para: {url}")

        response = requests.get(url)
        logger.info(f"Status da requisição: {response.status_code}")

        if response.status_code != 200:
            logger.error(f"Erro na requisição: Status {response.status_code}")
            raise requests.RequestException(f"Status code: {response.status_code}")

        data = json.loads(response.text)
        logger.info(f"Dados recebidos. Chaves principais: {list(data.keys())}")

        if data.get('result') != 'success':
            logger.error(f"API retornou erro: {data.get('error-type', 'Erro desconhecido')}")
            raise ValueError(f"Erro da API: {data.get('error-type')}")

        with open(file_path, 'w') as f:
            json.dump(data, f)

        logger.info(f"Dados salvos com sucesso em: {file_path}")
        logger.info("Processo de extração concluído com sucesso")

    except Exception as e:
        logger.error(f"Erro: {e}")
        raise

if __name__ == "__main__":
    main()