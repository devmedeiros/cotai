import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
silver_path = os.path.join(BASE_DIR, 'data', 'silver', 'silver.parquet')
silver_code_path = os.path.join(BASE_DIR, 'data', 'silver', 'currency_code_country.csv')
gold_path = os.path.join(BASE_DIR, 'data', 'gold', 'gold.parquet')

df = pd.read_parquet(silver_path)
df = df.sort_values(['moeda', 'timestamp'])

# Variações percentuais
df['var_1d'] = df.groupby('moeda')['taxa'].pct_change(1) * 100
df['var_7d'] = df.groupby('moeda')['taxa'].pct_change(7) * 100
df['var_30d'] = df.groupby('moeda')['taxa'].pct_change(30) * 100

# Médias móveis
df['ma_7d'] = df.groupby('moeda')['taxa'].rolling(window=7, min_periods=1).mean().reset_index(0, drop=True)
df['ma_30d'] = df.groupby('moeda')['taxa'].rolling(window=30, min_periods=1).mean().reset_index(0, drop=True)

# Volatilidade (desvio padrão móvel)
df['volatilidade_7d'] = df.groupby('moeda')['taxa'].rolling(window=7, min_periods=2).std().reset_index(0, drop=True)
df['volatilidade_30d'] = df.groupby('moeda')['taxa'].rolling(window=30, min_periods=2).std().reset_index(0, drop=True)

# Diferença absoluta com média móvel
df['diff_ma_7d'] = abs(df['taxa'] - df['ma_7d'])
df['diff_ma_30d'] = abs(df['taxa'] - df['ma_30d'])

def calcular_dias_consecutivos(series):
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

# Aplicar a função
df['dias_consecutivos'] = df.groupby('moeda')['taxa'].transform(calcular_dias_consecutivos)

# Tendência simples (baseada na variação de 7 dias)
def classificar_tendencia(var_7d):
    if pd.isna(var_7d):
        return 'indefinido'
    elif var_7d > 2:
        return 'alta'
    elif var_7d < -2:
        return 'baixa'
    else:
        return 'estável'

df['tendencia'] = df['var_7d'].apply(classificar_tendencia)

# Intensidade da tendência (baseada na magnitude da variação)
def classificar_intensidade(var_7d):
    if pd.isna(var_7d):
        return 'indefinido'
    abs_var = abs(var_7d)
    if abs_var > 5:
        return 'forte'
    elif abs_var > 2:
        return 'moderada'
    else:
        return 'fraca'

df['intensidade_tendencia'] = df['var_7d'].apply(classificar_intensidade)

# Classificação de momentum (comparando variação 1d vs 7d)
def classificar_momentum(var_1d, var_7d):
    if pd.isna(var_1d) or pd.isna(var_7d):
        return 'indefinido'
    
    if var_7d > 0:  # Tendência de alta
        return 'acelerando' if var_1d > var_7d/7 else 'desacelerando'
    elif var_7d < 0:  # Tendência de baixa
        return 'acelerando' if var_1d < var_7d/7 else 'desacelerando'
    else:
        return 'neutro'

df['momentum'] = df.apply(lambda row: classificar_momentum(row['var_1d'], row['var_7d']), axis=1)

# Status da taxa vs média móvel
df['status_ma_7d'] = df.apply(lambda row: 'acima' if row['taxa'] > row['ma_7d'] else 'abaixo', axis=1)
df['status_ma_30d'] = df.apply(lambda row: 'acima' if row['taxa'] > row['ma_30d'] else 'abaixo', axis=1)

# Categoria de variação (baseada na volatilidade de 7 dias)
def classificar_volatilidade(volatilidade_7d):
    if pd.isna(volatilidade_7d):
        return 'indefinido'
    elif volatilidade_7d > 0.1:  # Ajuste esses thresholds conforme seus dados
        return 'muito_volátil'
    elif volatilidade_7d > 0.05:
        return 'normal'
    else:
        return 'estável'

df['categoria_variacao'] = df['volatilidade_7d'].apply(classificar_volatilidade)

codes = pd.read_csv(silver_code_path, sep='\t', encoding='utf-8')
codes.columns = ['moeda','nm_moeda','nm_pais_en']

df0 = df.merge(codes, left_on='moeda', right_on='moeda')
df0.to_parquet(gold_path)