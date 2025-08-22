# cotai

Projeto Final da disciplina **Python Programming for Data Engineers**.

Os requisitos do projeto foram propostos pelo professor **Eduardo Miranda**.

## Objetivo
 1. Coleta taxas de câmbio da API https://www.exchangerate-api.com/.
 2. Processar, validar e armazenar os dados em diferentes camadas (raw, silver, gold).
 3. Integra uma LLM (ex.: ChatGPT) para gerar resumos e insights em linguagem natural, voltados a usuários de negócio.

## Fluxo do Projeto

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