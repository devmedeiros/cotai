# cotai

Projeto Final da disciplina **Python Programming for Data Engineers**. O relatório em streamlit está disponível aqui https://appcotai.streamlit.app .

Os requisitos do projeto foram propostos pelo professor **Eduardo Miranda**.

## Objetivo
 1. Coleta taxas de câmbio da API https://www.exchangerate-api.com/.
 2. Processar, validar e armazenar os dados em diferentes camadas (raw, silver, gold).
 3. Integra uma LLM (ex.: ChatGPT) para gerar resumos e insights em linguagem natural, voltados a usuários de negócio.

## Setup e Configuração

### Pré-requisitos
 - Python 3.10.12

### Instalação
 1. Clone o repositório

```bash
git clone https://github.com/devmedeiros/cotai.git
```

 2. Instale as dependências

```bash
pip install -r requirements.txt
```

### Configuração das APIs

**1. Exchange Rate API**
 1. Acesse https://www.exchangerate-api.com/
 2. Faça o cadastro gratuito
 3. Copie sua API key do dashboard
<img width="858" height="261" alt="Captura de tela de 2025-08-28 20-18-14" src="https://github.com/user-attachments/assets/c25f7859-51b8-4a55-8fa0-f4142f3194b3" />



**2. Google Gemini API**
 1. Acesse https://ai.google.dev/
 2. Crie uma conta Google se necessário
 3. Gere uma API key no Google AI Studio
 4. Copie sua API key

**3. Arquivo .env**
Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:
```bash
API_KEY="CHAVE_DE_API"
GEMINI_API_KEY="API_DO_GEMINI"
```
Na qual `API_KEY` é sua chave do Exchange Rate API e a `GEMINI_API_KEY` é a sua chave do Gemini.

### Execução

**Pipeline ETL**

Execute o processo completo de ETL com análise do Gemini: 

```bash
python cotai/pipeline.py
```

**Dashboard Streamlit**

Para visualizar o relatório interativo:

```bash
streamlit run app/main.py
```

## Fluxo do Projeto (conforme as instruções do professor)

### 1. Ingestão (Ingest)
 - Coletar dados da API https://www.exchangerate-api.com/.
 - Salvar resposta JSON bruta em `/raw/` com nome dos arquivos padronizados em `YYYY-MM-DD`.

_💡 Configuração via `.env` ou `YAML`, nunca hardcode de chaves/API._

### 2. Transformação (Transform)
 - Normalizar os dados (moeda, taxa, base_currency, timestamp).
 - Garantir qualidade (nenhuma taxa negativa ou nula).
 - Armazenar em `/silver/`.

### 3. Carga (Load)
 - Gravar dados finais em formato Parquet em `/gold/`.
 - (Opcional) Carregar também em banco relacional (Postgres/MySQL).

### 4. Enriquecimento com LLM (substitui orquestração)
Usar o ChatGPT para interpretar as cotações e gerar um resumo em linguagem natural, como:
 - “O Euro está 5% mais valorizado em relação ao mês passado.”
 - “A volatilidade do JPY em relação ao USD está acima da média.”

Criação de Explicações para Usuários de Negócio. Passar o dataset diário para o ChatGPT e pedir uma explicação executiva: 
 - “Explique em termos simples como está a variação das 5 principais moedas frente ao Real hoje.”

### 5. Testes e Observabilidade
 - Testes unitários (ex.: validação de taxas numéricas).
 - Logging estruturado durante ingestão e transformação. Usar biblioteca estruturada (logging ou structlog) com níveis (INFO, ERROR).
 - (Opcional) Logging do prompt/response do ChatGPT para auditoria.

### 6. Entregáveis
 - Código no GitHub.
 - README documentando setup, execução e como configurar chaves da API de câmbio e do LLM escolhido.
 - Arquivos finais (`/gold/`) contendo:
    * Dados limpos em Parquet.
    * Relatórios/insights da LLM.
 - (Opcional) Dashboard simples em Streamlit/Metabase.
