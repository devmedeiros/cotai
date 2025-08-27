import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google import genai
import pandas as pd
from datetime import date, datetime
from dotenv import load_dotenv
from pathlib import Path
from utils.logger import setup_logger

logger = setup_logger(__name__)

def salvar_insight_diario(data_referencia, insights_texto, versao_prompt="v1.0", moedas_analisadas=""):
    """Salva insight diário no arquivo parquet"""
    try:
        logger.info(f"Salvando insight para data: {data_referencia}")
        
        nova_linha = pd.DataFrame({
            'data': [pd.to_datetime(data_referencia)],
            'insights_texto': [insights_texto],
            'timestamp_criacao': [datetime.now()],
            'versao_prompt': [versao_prompt],
            'moedas_analisadas': [moedas_analisadas]
        })
        
        if Path(insight_path).exists():
            logger.info("Arquivo de insights existente encontrado")
            df_existente = pd.read_parquet(insight_path)
            
            if data_referencia in df_existente['data'].dt.date.values:
                logger.warning(f"Insight para {data_referencia} já existe. Pulando...")
                return False
            
            df_final = pd.concat([df_existente, nova_linha], ignore_index=True)
            logger.info(f"Combinando com dados existentes: {len(df_existente)} + 1 registros")
        else:
            logger.info("Criando novo arquivo de insights")
            df_final = nova_linha
        
        # Criar diretório se não existir
        os.makedirs(os.path.dirname(insight_path), exist_ok=True)
        
        df_final.to_parquet(insight_path, index=False)
        logger.info(f"Insight para {data_referencia} salvo com sucesso! Total: {len(df_final)} registros")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao salvar insight: {e}")
        raise

def formatar_dados_prompt(df_dia):
    """Formata dados do DataFrame para o prompt"""
    try:
        logger.info(f"Formatando dados para {len(df_dia)} moedas")
        
        texto_dados = []
        for _, row in df_dia.iterrows():
            linha = f"- {row['nm_pais_en']} ({row['moeda']}): tendência {row['tendencia']}, intensidade {row['intensidade_tendencia']}, {row['status_ma_7d']} da média móvel 7d, volatilidade {row['categoria_variacao']}"
            texto_dados.append(linha)
        
        resultado = "\n".join(texto_dados)
        logger.info("Dados formatados com sucesso")
        return resultado
        
    except Exception as e:
        logger.error(f"Erro ao formatar dados: {e}")
        raise

def main():
    try:
        logger.info("Iniciando processo de geração de insights")
        
        # Carregar variáveis de ambiente
        load_dotenv()
        GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
        
        if not GEMINI_API_KEY:
            logger.error("GEMINI_API_KEY não encontrada nas variáveis de ambiente")
            raise ValueError("GEMINI_API_KEY não configurada")
        
        logger.info("GEMINI_API_KEY carregada com sucesso")
        
        # Configurar caminhos e moedas
        moedas = ['EUR','USD', 'RUB', 'CNY', 'INR', 'ZAR', 'GBP']
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        gold_path = os.path.join(BASE_DIR, 'data', 'gold', 'gold.parquet')
        
        global insight_path
        insight_path = os.path.join(BASE_DIR, 'data', 'gold', 'insights_diarios.parquet')
        
        logger.info(f"Arquivo gold: {gold_path}")
        logger.info(f"Arquivo insights: {insight_path}")
        logger.info(f"Moedas selecionadas: {moedas}")
        
        # Verificar se arquivo gold existe
        if not os.path.exists(gold_path):
            logger.error(f"Arquivo gold não encontrado: {gold_path}")
            raise FileNotFoundError(f"Arquivo não encontrado: {gold_path}")
        
        # Carregar dados
        logger.info("Carregando dados do gold layer")
        df = pd.read_parquet(gold_path)
        logger.info(f"Dados carregados: {len(df)} registros")
        
        # Filtrar dados
        today = date.today()
        logger.info(f"Filtrando dados para hoje: {today}")
        df_selecao = df[(df.moeda.isin(moedas)) & (df.timestamp.dt.date >= today)]
        
        if df_selecao.empty:
            logger.warning("Nenhum dado encontrado para hoje. Tentando dados mais recentes...")
            df_selecao = df[df.moeda.isin(moedas)].groupby('moeda').tail(1)
            
        if df_selecao.empty:
            logger.error("Nenhum dado encontrado para as moedas selecionadas")
            raise ValueError("Sem dados disponíveis para análise")
        
        logger.info(f"Dados filtrados: {len(df_selecao)} registros para {len(df_selecao['moeda'].unique())} moedas")
        
        # Formatar dados para prompt
        dados_formatados = formatar_dados_prompt(df_selecao)
        
        # Criar prompt
        timestamp = today
        prompt = f'''Você é um analista financeiro especializado em câmbio. Com base nos dados fornecidos sobre taxas de câmbio em relação ao Real Brasileiro (BRL), gere um parágrafo em português analisando a situação das principais moedas.

Data de análise: {timestamp}

Dados das moedas:
{dados_formatados}

Instruções:
- Escreva um parágrafo de 3-4 frases em português
- Foque nas moedas mais relevantes (USD, EUR, GBP como prioritárias)
- Mencione tendências interessantes ou padrões que se destacam
- Use linguagem profissional, mas acessível
- Evite repetir informações óbvias
- Destaque contrastes entre moedas quando relevante
- Use formatação markdown: **negrito** para nomes de países/moedas importantes, *itálico* para enfatizar tendências

Exemplo de tom: "O **Euro** apresenta tendência de *alta* com intensidade forte, mantendo-se **acima** da média móvel de 7 dias, enquanto o **Dólar americano** mostra movimento *estável* mas permanece **abaixo** das médias históricas..."

Responda apenas com o parágrafo em markdown, sem introduções ou explicações adicionais.'''
        
        logger.info("Enviando requisição para Gemini API")
        
        # Chamar API Gemini
        client = genai.Client(api_key=GEMINI_API_KEY)
        response = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
        
        if not response.text:
            logger.error("API retornou resposta vazia")
            raise ValueError("Resposta vazia da API")
        
        logger.info("Resposta recebida da API com sucesso")
        logger.info(f"Tamanho da resposta: {len(response.text)} caracteres")
        
        # Salvar insight
        sucesso = salvar_insight_diario(
            data_referencia=str(today),
            insights_texto=response.text,
            moedas_analisadas=', '.join(moedas)
        )
        
        if sucesso:
            logger.info("Processo de geração de insights concluído com sucesso")
        else:
            logger.info("Insight já existia para hoje")
            
    except FileNotFoundError as e:
        logger.error(f"Arquivo não encontrado: {e}")
        raise
    except ValueError as e:
        logger.error(f"Erro de valor: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")
        raise

if __name__ == "__main__":
    main()