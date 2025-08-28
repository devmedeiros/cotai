import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.set_page_config(
    page_title='CotAI - Dashboard de Câmbio',
    layout='wide',
    page_icon='💱'
)

@st.cache_data
def load_data(ttl='3h'):
    return pd.read_parquet('data/gold/gold.parquet')

def carregar_insight_do_dia(data_referencia):
    df = pd.read_parquet('data/gold/insights_diarios.parquet')
    insight = df[df['data'].dt.date == pd.to_datetime(data_referencia).date()]
    if insight.empty:
        return None
    return insight.iloc[0]['insights_texto']

st.title('💱 CotAI - Dashboard de Câmbio')

df = load_data()

data_maxima = df['timestamp'].max()
moedas_principais = ['USD', 'GBP', 'EUR', 'CNY', 'INR', 'RUB', 'ZAR']

df_filtrado = df[
    (df['timestamp'] == data_maxima) & 
    (df['moeda'].isin(moedas_principais))
].sort_values('moeda')

c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
colunas = [c1, c2, c3, c4, c5, c6, c7]

for i, (_, row) in enumerate(df_filtrado.iterrows()):
    if i < len(colunas):
        variacao = row.get('var_1d', 0) if pd.notna(row.get('var_1d')) else 0
        
        colunas[i].metric(
            label=f"{row['moeda']}",
            value=f"R$ {row['taxa']:.4f}",
            delta=f"{variacao:.2f}%" if variacao != 0 else None
        )

st.subheader('Análise Diária de IA')

insight_texto = carregar_insight_do_dia(data_maxima.strftime('%Y-%m-%d'))
if insight_texto:
    st.markdown(insight_texto)
    st.caption(f"Análise gerada para {data_maxima.strftime('%d/%m/%Y')}")
else:
    st.info("Insight diário ainda não disponível para hoje.")

principais = ['USD', 'EUR', 'GBP']
brics = ['CNY', 'INR', 'RUB', 'ZAR']

tab1, tab2, tab3 = st.tabs(["Média Móvel - Principais", "Média Móvel - BRICS", 'Base de Dados'])

with tab1:
    fig_principais = make_subplots(rows=1, cols=3, subplot_titles=principais)
    
    for i, moeda in enumerate(principais):
        dados_moeda = df[df['moeda'] == moeda].sort_values('timestamp')
        fig_principais.add_trace(go.Scatter(x=dados_moeda['timestamp'], y=dados_moeda['taxa'], mode='lines', showlegend=False), row=1, col=i+1)
        fig_principais.add_trace(go.Scatter(x=dados_moeda['timestamp'], y=dados_moeda['ma_7d'], mode='lines', line=dict(dash='dash', color='gray'), showlegend=False), row=1, col=i+1)
    
    fig_principais.update_layout(height=400)
    st.plotly_chart(fig_principais, use_container_width=True)

with tab2:
    fig_brics = make_subplots(rows=2, cols=2, subplot_titles=brics)
    
    for i, moeda in enumerate(brics):
        row = (i // 2) + 1
        col = (i % 2) + 1
        dados_moeda = df[df['moeda'] == moeda].sort_values('timestamp')
        fig_brics.add_trace(go.Scatter(x=dados_moeda['timestamp'], y=dados_moeda['taxa'], mode='lines', showlegend=False), row=row, col=col)
        fig_brics.add_trace(go.Scatter(x=dados_moeda['timestamp'], y=dados_moeda['ma_7d'], mode='lines', line=dict(dash='dash', color='gray'), showlegend=False), row=row, col=col)
    
    fig_brics.update_layout(height=500)
    st.plotly_chart(fig_brics, use_container_width=True)

with tab3:
    cola, colb = st.columns([1,3])
    with cola:
        pais = st.selectbox('País (em inglês)', sorted(df.nm_pais_en.dropna().unique()), 
        placeholder='Selecione', index=None)
        moeda = st.selectbox('Moeda (sigla)', sorted(df.moeda.dropna().unique()), 
        placeholder='Selecione', index=None)
        categoria = st.pills('Categoria de Variação', df.categoria_variacao.unique())

    dff = df.copy()
    if pais:
        dff = dff[dff.nm_pais_en == pais]
    if moeda:
        dff = dff[dff.moeda == moeda]
    if categoria:
        dff = dff[dff.categoria_variacao == categoria]

    if dff.empty:
        colb.info('Sua seleção não retornou nenhum dado.')
    else:
        colb.dataframe(dff, hide_index=True)
