import streamlit as st
import duckdb
import os
import polars as pl

# Configuração de Layout do Dashboard de Engenharia
st.set_page_config(
    page_title="Watchdog Data Observability",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

DELTA_LAKE_PATH = os.path.abspath("data/delta_lake")

def load_lakehouse_metrics():
    """
    Consulta diretamente os metadados físicos do Delta Lake usando o DuckDB.
    """
    if not os.path.exists(DELTA_LAKE_PATH) or not os.listdir(DELTA_LAKE_PATH):
        return None
        
    con = duckdb.connect()
    
    query = f"""
        SELECT 
            status,
            COUNT(*) as total_transacoes,
            ROUND(SUM(amount), 2) as volume_total,
            ROUND(AVG(amount), 2) as ticket_medio
        FROM parquet_scan('{DELTA_LAKE_PATH}/**/*.parquet')
        GROUP BY status
    """
    return con.execute(query).pl()

def get_recent_transactions():
    """
    Busca uma amostragem rápida das últimas transações salvas.
    """
    con = duckdb.connect()
    query = f"""
        SELECT transaction_id, account_source, account_destination, amount, timestamp, status, source_api
        FROM parquet_scan('{DELTA_LAKE_PATH}/**/*.parquet')
        ORDER BY timestamp DESC
        LIMIT 10
    """
    return con.execute(query).pl()

# --- FRONTEND STREAMLIT ---
st.title("🛡️ Project Watchdog | Data Observability & Quality")
st.markdown("Monitoramento de integridade e volumetria analítica em tempo real sobre infraestrutura portátil **Delta Lake + DuckDB**.")

st.sidebar.header("⚙️ Configurações do Lakehouse")
st.sidebar.info(f"**Storage Target:** \n`{DELTA_LAKE_PATH}`")

if st.sidebar.button("🔄 Forçar Recarga Analítica"):
    st.toast("Metadados recalculados via DuckDB!")

df_metrics = load_lakehouse_metrics()

if df_metrics is None:
    st.warning("⚠️ O Lakehouse local está vazio ou a pasta Delta Lake não foi inicializada. Execute `python main.py` para gerar os primeiros dados.")
else:
    total_records = df_metrics["total_transacoes"].sum()
    total_volume = df_metrics["volume_total"].sum()
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(label="📊 Total Ingerido (Registros)", value=f"{total_records:,}")
    with col2:
        st.metric(label="💰 Volume Total Financeiro", value=f"R\$ {total_volume:,.2f}")
    with col3:
        suspicious_count = df_metrics.filter(pl.col("status") == "SUSPICIOUS")["total_transacoes"].sum()
        st.metric(label="🚨 Alertas de Fraude Ativos", value=int(suspicious_count), delta="Análise de Risco", delta_color="inverse")
    with col4:
        failed_count = df_metrics.filter(pl.col("status") == "FAILED")["total_transacoes"].sum()
        st.metric(label="❌ Falhas Operacionais Ingeridas", value=int(failed_count))
        
    st.markdown("---")
    
    # CORREÇÃO DA API DO STREAMLIT PASSANDO O PARÂMETRO 2 OBRIGATÓRIO
    left_col, right_col = st.columns(2)
    
    with left_col:
        st.subheader("📈 Distribuição Volumétrica por Status")
        st.bar_chart(
            data=df_metrics.to_pandas().set_index("status")[["total_transacoes"]],
            use_container_width=True
        )
        
    with right_col:
        st.subheader("🔍 Métricas Consolidadas (DuckDB Engine)")
        st.dataframe(df_metrics.to_pandas(), use_container_width=True, hide_index=True)
        
    st.markdown("---")
    
    st.subheader("📋 Últimas 10 Transações Gravadas na Camada ACID")
    df_recent = get_recent_transactions()
    st.dataframe(df_recent.to_pandas(), use_container_width=True, hide_index=True)
    
    st.success("🟢 Data Quality Gateway: Ativo e monitorando Schema Drift de APIs de streaming.")
