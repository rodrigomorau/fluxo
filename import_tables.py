import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

def criar_engine_postgres(prefixo_env: str):
    host = os.getenv(f"{prefixo_env}_HOST")
    user = os.getenv(f"{prefixo_env}_USER")
    password = os.getenv(f"{prefixo_env}_PASS")
    database = os.getenv(f"{prefixo_env}_DB")
    
    if not all([host, user, password, database]):
        raise ValueError(f"Variáveis de ambiente incompletas para prefixo '{prefixo_env}'")
    
    return create_engine(f"postgresql://{user}:{password}@{host}:5432/{database}")

# Engines
engine_neon = criar_engine_postgres("NEON")
engine_local = criar_engine_postgres("LOCAL")

tabelas_para_copiar = ["dim_fluxo"]

for tabela in tabelas_para_copiar:
    print(f"Copiando tabela: {tabela}")
    df = pd.read_sql(f'SELECT * FROM fluxo."{tabela}"', engine_neon)
    df.to_sql(tabela, engine_local, schema="fluxo", if_exists="replace", index=False)
    print(f"Tabela {tabela} copiada com sucesso!\n")

print("Todas as tabelas foram copiadas com sucesso!")
