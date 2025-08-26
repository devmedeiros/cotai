import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from utils.logger import setup_logger

logger = setup_logger(__name__)

def calcular_dias_consecutivos(series):
    """Calcula dias consecutivos de mudança na mesma direção"""
    # Calcular mudanças
    mudancas = series.diff().fillna(0)
    direcao = mudancas.apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    # Identificar grupos consecutivos
    grupos = (direcao != direcao.shift(1)).cumsum()
    # Calcular tamanho de cada grupo
    tamanhos = grupos.map(grupos.value_counts())
    # Aplicar regra: se direção é 0, dias consecutivos = 0
    dias_consecutivos = tamanhos.where(direcao != 0, 0)
    return dias_consecutivos

def classificar_tendencia(var_7d):
    """Classifica tendência baseada na variação de 7 dias"""
    if pd.isna(var_7d):
        return 'indefinido'
    elif var_7d > 2:
        return 'alta'
    elif var_7d < -2:
        return 'baixa'
    else:
        return 'estável'

def classificar_intensidade(var_7d):
    """Classifica intensidade da tendência"""
    if pd.isna(var_7d):
        return 'indefinido'
    abs_var = abs(var_7d)
    if abs_var > 5:
        return 'forte'
    elif abs_var > 2:
        return 'moderada'
    else:
        return 'fraca'

def classificar_momentum(var_1d, var_7d):
    """Classifica momentum comparando variação 1d vs 7d"""
    if pd.isna(var_1d) or pd.isna(var_7d):
        return 'indefinido'
    if var_7d > 0:  # Tendência de alta
        return 'acelerando' if var_1d > var_7d/7 else 'desacelerando'
    elif var_7d < 0:  # Tendência de baixa
        return 'acelerando' if var_1d < var_7d/7 else 'desacelerando'
    else:
        return 'neutro'

def classificar_volatilidade(volatilidade_7d):
    """Classifica categoria de volatilidade"""
    if pd.isna(volatilidade_7d):
        return 'indefinido'
    elif volatilidade_7d > 0.1:
        return 'muito_volátil'
    elif volatilidade_7d > 0.05:
        return 'normal'
    else:
        return 'estável'

def main():
    try:
        logger.info("Iniciando transformação dos dados para gold layer")
        
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        silver_path = os.path.join(BASE_DIR, 'data', 'silver', 'silver.parquet')
        silver_code_path = os.path.join(BASE_DIR, 'data', 'silver', 'currency_code_country.csv')
        gold_path = os.path.join(BASE_DIR, 'data', 'gold', 'gold.parquet')
        
        logger.info(f"Arquivo silver: {silver_path}")
        logger.info(f"Arquivo códigos: {silver_code_path}")
        logger.info(f"Arquivo gold: {gold_path}")
        
        # Verificar se arquivos existem
        if not os.path.exists(silver_path):
            logger.error(f"Arquivo silver não encontrado: {silver_path}")
            raise FileNotFoundError(f"Arquivo não encontrado: {silver_path}")
        
        if not os.path.exists(silver_code_path):
            logger.error(f"Arquivo de códigos não encontrado: {silver_code_path}")
            raise FileNotFoundError(f"Arquivo não encontrado: {silver_code_path}")
        
        # Carregar dados
        logger.info("Carregando dados do silver layer")
        df = pd.read_parquet(silver_path)
        logger.info(f"Dados silver carregados: {len(df)} registros, {len(df['moeda'].unique())} moedas únicas")
        
        logger.info("Ordenando dados por moeda e timestamp")
        df = df.sort_values(['moeda', 'timestamp'])
        
        # Calcular variações percentuais
        logger.info("Calculando variações percentuais")
        df['var_1d'] = df.groupby('moeda')['taxa'].pct_change(1) * 100
        df['var_7d'] = df.groupby('moeda')['taxa'].pct_change(7) * 100
        df['var_30d'] = df.groupby('moeda')['taxa'].pct_change(30) * 100
        
        # Calcular médias móveis
        logger.info("Calculando médias móveis")
        df['ma_7d'] = df.groupby('moeda')['taxa'].rolling(window=7, min_periods=1).mean().reset_index(0, drop=True)
        df['ma_30d'] = df.groupby('moeda')['taxa'].rolling(window=30, min_periods=1).mean().reset_index(0, drop=True)
        
        # Calcular volatilidade
        logger.info("Calculando volatilidade")
        df['volatilidade_7d'] = df.groupby('moeda')['taxa'].rolling(window=7, min_periods=2).std().reset_index(0, drop=True)
        df['volatilidade_30d'] = df.groupby('moeda')['taxa'].rolling(window=30, min_periods=2).std().reset_index(0, drop=True)
        
        # Diferença absoluta com média móvel
        logger.info("Calculando diferenças com médias móveis")
        df['diff_ma_7d'] = abs(df['taxa'] - df['ma_7d'])
        df['diff_ma_30d'] = abs(df['taxa'] - df['ma_30d'])
        
        # Calcular dias consecutivos
        logger.info("Calculando dias consecutivos")
        df['dias_consecutivos'] = df.groupby('moeda')['taxa'].transform(calcular_dias_consecutivos)
        
        # Classificações
        logger.info("Aplicando classificações de tendência")
        df['tendencia'] = df['var_7d'].apply(classificar_tendencia)
        df['intensidade_tendencia'] = df['var_7d'].apply(classificar_intensidade)
        df['momentum'] = df.apply(lambda row: classificar_momentum(row['var_1d'], row['var_7d']), axis=1)
        
        # Status vs médias móveis
        logger.info("Calculando status vs médias móveis")
        df['status_ma_7d'] = df.apply(lambda row: 'acima' if row['taxa'] > row['ma_7d'] else 'abaixo', axis=1)
        df['status_ma_30d'] = df.apply(lambda row: 'acima' if row['taxa'] > row['ma_30d'] else 'abaixo', axis=1)
        
        # Categoria de variação
        logger.info("Classificando volatilidade")
        df['categoria_variacao'] = df['volatilidade_7d'].apply(classificar_volatilidade)
        
        # Carregar códigos das moedas
        logger.info("Carregando códigos das moedas")
        codes = pd.read_csv(silver_code_path, sep='\t', encoding='utf-8')
        codes.columns = ['moeda','nm_moeda','nm_pais_en']
        logger.info(f"Códigos carregados: {len(codes)} registros")
        
        # Fazer merge
        logger.info("Fazendo merge com códigos das moedas")
        df_before_merge = len(df)
        df0 = df.merge(codes, left_on='moeda', right_on='moeda', how='left')
        df_after_merge = len(df0)
        
        if df_before_merge != df_after_merge:
            logger.warning(f"Mudança no número de registros após merge: {df_before_merge} -> {df_after_merge}")
        
        # Verificar moedas sem match
        moedas_sem_codigo = df0[df0['nm_moeda'].isna()]['moeda'].unique()
        if len(moedas_sem_codigo) > 0:
            logger.warning(f"Moedas sem código encontrado: {list(moedas_sem_codigo)}")
        
        # Criar diretório gold se necessário
        os.makedirs(os.path.dirname(gold_path), exist_ok=True)
        logger.info(f"Diretório gold criado/verificado: {os.path.dirname(gold_path)}")
        
        # Salvar arquivo gold
        logger.info("Salvando arquivo gold")
        df0.to_parquet(gold_path, index=False)
        logger.info(f"Arquivo gold salvo com {len(df0)} registros: {gold_path}")
        
        # Estatísticas finais
        logger.info("=== ESTATÍSTICAS FINAIS ===")
        logger.info(f"Total de registros: {len(df0)}")
        logger.info(f"Moedas únicas: {len(df0['moeda'].unique())}")
        logger.info(f"Período: {df0['timestamp'].min()} a {df0['timestamp'].max()}")
        logger.info("Transformação para gold layer concluída com sucesso")
        
    except FileNotFoundError as e:
        logger.error(f"Arquivo não encontrado: {e}")
        raise
    except pd.errors.EmptyDataError as e:
        logger.error(f"Arquivo vazio ou inválido: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")
        raise

if __name__ == "__main__":
    main()