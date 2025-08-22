import pandas as pd
from datetime import date, datetime
import os

today = str(date.today())

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
raw_path = os.path.join(BASE_DIR, 'data', 'raw', f'{today}.json')
silver_path = os.path.join(BASE_DIR, 'data', 'silver', 'silver.parquet')

df_new = pd.read_json(raw_path)
df_new.reset_index(inplace=True)
df_new['timestamp'] = df_new['time_last_update_unix'].apply(lambda x: datetime.fromtimestamp(x))
df_new = df_new[['index','conversion_rates','base_code','timestamp']]
df_new.columns = ['moeda', 'taxa', 'base_currency', 'timestamp']
df_new = df_new[~(df_new.taxa <= 0)]

os.makedirs(os.path.dirname(silver_path), exist_ok=True)

if os.path.exists(silver_path):
    df_existing = pd.read_parquet(silver_path)
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    df_combined.drop_duplicates(subset=['moeda','timestamp'], inplace=True)
else:
    df_combined = df_new

df_combined.to_parquet(silver_path, index=False)