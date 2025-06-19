import pandas as pd
import os
import re
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# === Carregar variáveis do .env ===
load_dotenv()

db_user = os.getenv('LOCAL_USER')
db_pass = os.getenv('LOCAL_PASS')
db_host = os.getenv('LOCAL_HOST')
db_port = os.getenv('LOCAL_PORT')
db_name = os.getenv('LOCAL_DB')

# === Criar conexão com o banco ===
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

# === Caminho do arquivo Excel ===
file_path = r'D:\01-Estudos\00-BaseDados\01-FluxoCaixa\07-Santander\01-Input\SantanderExtrato.xlsx'

# === Leitura do arquivo Excel ===
try:
    df = pd.read_excel(file_path, dtype=str)

    # Renomear colunas
    df.columns = ["data_lanc", "descricao", "valor", "idcategoria", "observacao"]

    # === Corrigir valores monetários no formato brasileiro ===
    def tratar_valor_brasileiro(valor_str):
        if pd.isna(valor_str):
            return 0.0
        valor_str = str(valor_str).strip()
        if re.match(r"^\d{1,3}(\.\d{3})+,\d{2}$", valor_str):
            valor_str = valor_str.replace(".", "")
        return float(valor_str.replace(",", "."))

    df["valor"] = df["valor"].apply(tratar_valor_brasileiro)

    # Converter data
    df["data_lanc"] = pd.to_datetime(df["data_lanc"], dayfirst=True, errors="coerce")

    # Adicionar colunas adicionais
    df["nome_arquivo"] = os.path.basename(file_path)
    df["data_carga"] = pd.to_datetime('today').normalize()

    # Selecionar colunas finais
    df_final = df[[
        "data_lanc", "descricao", "valor", "idcategoria", "observacao", "nome_arquivo", "data_carga"
    ]]

    # === Substituir dados da tabela existente ===
    with engine.connect() as conn:
        check_table_query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'fluxo'
                AND table_name = 'santander'
            );
        """)
        table_exists = conn.execute(check_table_query).scalar()

        if table_exists:
            delete_query = text("DELETE FROM fluxo.santander;")
            conn.execute(delete_query)
            conn.commit()
            print("Dados antigos excluídos da tabela fluxo.santander.")

        # Inserir novos dados
        df_final.to_sql('santander', engine, schema='fluxo', if_exists='append', index=False)
        print(f"{len(df_final)} linhas inseridas na tabela fluxo.santander.")

except Exception as e:
    print(f"Erro ao processar o arquivo: {e}")
