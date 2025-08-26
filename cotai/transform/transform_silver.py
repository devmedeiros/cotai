import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import date, datetime
import json
from utils.logger import setup_logger

logger = setup_logger(__name__)

def main():
    try:
        logger.info("Iniciando transformação dos dados para silver layer")
        
        today = str(date.today())
        logger.info(f"Processando dados do dia: {today}")
        
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        raw_path = os.path.join(BASE_DIR, 'data', 'raw', f'{today}.json')
        silver_path = os.path.join(BASE_DIR, 'data', 'silver', 'silver.parquet')
        
        logger.info(f"Arquivo raw: {raw_path}")
        logger.info(f"Arquivo silver: {silver_path}")
        
        # Verificar se arquivo raw existe
        if not os.path.exists(raw_path):
            logger.error(f"Arquivo raw não encontrado: {raw_path}")
            raise FileNotFoundError(f"Arquivo não encontrado: {raw_path}")
        
        # Ler e inspecionar o JSON primeiro
        logger.info("Lendo arquivo JSON")
        with open(raw_path, 'r') as f:
            json_data = json.load(f)
        
        logger.info(f"Estrutura do JSON: {type(json_data)}")
        logger.info(f"Chaves principais: {list(json_data.keys()) if isinstance(json_data, dict) else 'N/A'}")
        
        # Processar baseado na estrutura do JSON
        if isinstance(json_data, dict) and 'conversion_rates' in json_data:
            logger.info("Processando JSON com estrutura de conversion_rates")
            rates = json_data['conversion_rates']
            df_new = pd.DataFrame(list(rates.items()), columns=['index', 'conversion_rates'])
            df_new['base_code'] = json_data.get('base_code', '')
            df_new['time_last_update_unix'] = json_data.get('time_last_update_unix', 0)
        else:
            logger.info("Tentando leitura direta com pd.read_json")
            df_new = pd.read_json(raw_path)
        
        logger.info(f"Dados carregados: {len(df_new)} registros")
        
        # Transformações
        logger.info("Iniciando transformações dos dados")
        df_new.reset_index(drop=True, inplace=True)
        
        # Verificar se coluna time_last_update_unix existe
        if 'time_last_update_unix' not in df_new.columns:
            logger.error("Coluna 'time_last_update_unix' não encontrada")
            logger.info(f"Colunas disponíveis: {list(df_new.columns)}")
            raise KeyError("Coluna 'time_last_update_unix' não encontrada")
        
        df_new['timestamp'] = df_new['time_last_update_unix'].apply(lambda x: datetime.fromtimestamp(x))
        df_new = df_new[['index','conversion_rates','base_code','timestamp']]
        df_new.columns = ['moeda', 'taxa', 'base_currency', 'timestamp']
        
        # Filtrar taxas inválidas
        before_filter = len(df_new)
        df_new = df_new[~(df_new.taxa <= 0)]
        after_filter = len(df_new)
        logger.info(f"Filtro de taxas: {before_filter} -> {after_filter} registros")
        
        # Criar diretório se necessário
        os.makedirs(os.path.dirname(silver_path), exist_ok=True)
        logger.info(f"Diretório silver criado/verificado: {os.path.dirname(silver_path)}")
        
        # Combinar com dados existentes
        if os.path.exists(silver_path):
            logger.info("Arquivo silver existente encontrado, combinando dados")
            df_existing = pd.read_parquet(silver_path)
            logger.info(f"Dados existentes: {len(df_existing)} registros")
            
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            before_dedup = len(df_combined)
            df_combined.drop_duplicates(subset=['moeda','timestamp'], inplace=True)
            after_dedup = len(df_combined)
            logger.info(f"Deduplicação: {before_dedup} -> {after_dedup} registros")
        else:
            logger.info("Primeiro arquivo silver, criando novo")
            df_combined = df_new
        
        # Salvar arquivo final
        df_combined.to_parquet(silver_path, index=False)
        logger.info(f"Arquivo silver salvo com {len(df_combined)} registros: {silver_path}")
        logger.info("Transformação para silver layer concluída com sucesso")
        
    except FileNotFoundError as e:
        logger.error(f"Arquivo não encontrado: {e}")
        raise
    except KeyError as e:
        logger.error(f"Erro de estrutura de dados: {e}")
        raise
    except pd.errors.EmptyDataError as e:
        logger.error(f"Arquivo JSON vazio ou inválido: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")
        raise

if __name__ == "__main__":
    main()