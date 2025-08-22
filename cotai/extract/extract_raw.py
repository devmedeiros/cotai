import requests
import os
import json
from datetime import date
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('API_KEY')

today = str(date.today())

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
file_path = os.path.join(BASE_DIR, 'data', 'raw', f'{today}.json')

url = f'https://v6.exchangerate-api.com/v6/{API_KEY}/latest/BRL/'
response = requests.get(url)
data = json.loads(response.text)

with open(file_path, 'w') as f:
    json.dump(data, f)