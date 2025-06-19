import pandas as pd
import os
import re
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# === Carregar variáveis do ambiente ===
load_dotenv()

db_user = os.getenv('LOCAL_USER')
db_pass = os.getenv('LOCAL_PASS')
db_host = os.getenv('LOCAL_HOST')
db_port = os.getenv('LOCAL_PORT')
db_name = os.getenv('LOCAL_DB')

# === Criar engine de conexão com PostgreSQL ===
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

# === Caminho do arquivo ===
file_path = r'D:\01-Estudos\00-BaseDados\01-FluxoCaixa\04-XP\01-Input\CreditoXP.xlsx'


# === Função para tratar valor monetário brasileiro ===
def tratar_valor_brasileiro(valor_str):
    if pd.isna(valor_str):
        return 0.0
    valor_str = str(valor_str).strip()

    # Se houver mais de uma vírgula, mantém apenas a última
    if valor_str.count(',') > 1:
        partes = valor_str.split(',')
        valor_str = ''.join(partes[:-1]) + ',' + partes[-1]

    # Remove pontos e troca vírgula por ponto
    valor_str = valor_str.replace(',', '.')

    try:
        return float(valor_str)
    except:
        return 0.0

# === Leitura e processamento do arquivo ===
try:
    df = pd.read_excel(file_path, dtype=str)

    # Renomear colunas se necessário
    df.columns = [
        "data_lanc", "estabelecimento", "portador", "valor", "parcela",
        "data_vencimento", "idcategoria", "observacao", "transferencia"
    ]

    # Converter datas
    df["data_lanc"] = pd.to_datetime(df["data_lanc"], errors="coerce")
    df["data_vencimento"] = pd.to_datetime(df["data_vencimento"], errors="coerce")

    # Tratar coluna valor
    df["valor"] = df["valor"].apply(tratar_valor_brasileiro)

    # Colunas adicionais
    df["nome_arquivo"] = os.path.basename(file_path)
    df["data_carga"] = pd.to_datetime('today').normalize()

    # Reorganizar colunas finais
    colunas_finais = [
        "data_lanc", "estabelecimento", "valor", "parcela",
        "data_vencimento", "idcategoria", "data_carga","nome_arquivo"
    ]
    df_final = df[colunas_finais]

    # Remover linhas totalmente vazias nas colunas principais
    colunas_principais = [
        "data_lanc", "valor", "parcela",
        "data_vencimento", "idcategoria"
    ]
    df_final = df_final.dropna(subset=colunas_principais, how='all')

    # Também remove linhas onde a data ou valor estão vazios
    df_final = df_final.dropna(subset=["data_lanc", "valor"])


    # === Inserção no banco ===
    with engine.connect() as conn:
        check_query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'fluxo' AND table_name = 'xp'
            );
        """)
        if conn.execute(check_query).scalar():
            conn.execute(text("DELETE FROM fluxo.xp;"))
            conn.commit()
            print("Dados antigos excluídos da tabela fluxo.nubank.")

        # Inserir novos dados
        df_final.to_sql("xp", engine, schema="fluxo", if_exists="append", index=False)
        print(f"{len(df_final)} linhas inseridas na tabela fluxo.xp.")

except Exception as e:
    print(f"Erro ao processar o arquivo: {e}")
