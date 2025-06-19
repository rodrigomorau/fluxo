import pandas as pd
import os
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Carregar variáveis do .env
load_dotenv()

# Obter dados de conexão do .env
db_user = os.getenv('LOCAL_USER')
db_pass = os.getenv('LOCAL_PASS')
db_host = os.getenv('LOCAL_HOST')
db_port = os.getenv('LOCAL_PORT')
db_name = os.getenv('LOCAL_DB')

# Criar engine
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

# Caminho do arquivo
folder_path = r'D:\01-Estudos\00-BaseDados\01-FluxoCaixa\02-BB\01-Input'
file_name = 'BB_extrato.xlsx'
file_path = os.path.join(folder_path, file_name)

# Verificar se o arquivo existe
if not os.path.exists(file_path):
    print(f"Arquivo não encontrado: {file_path}")
    exit()

# Nomes fixos das colunas esperadas
nomes_colunas = [
    'data_lanc', 'origem', 'descricao', 'data_balancete', 'num_doc',
    'valor', 'idcategoria', 'observacao', 'idformapagto', 'dfluxo'
]

try:
    # Leitura e tratamento do DataFrame
    df = pd.read_excel(file_path, header=None)
    header_row = df[df.iloc[:, 0].astype(str).str.contains('data', case=False, na=False)].index[0]
    df = pd.read_excel(file_path, header=header_row)

    if len(df.columns) == len(nomes_colunas):
        df.columns = nomes_colunas
    else:
        raise ValueError("Número de colunas incompatível com os nomes definidos.")

    df['nome_arquivo'] = file_name
    df['data_lanc'] = pd.to_datetime(df['data_lanc'], errors='coerce', dayfirst=True)
    df = df.sort_values(by='data_lanc')
    df['data_carga'] = pd.to_datetime('today').normalize()

    colunas_finais = [
        'data_lanc', 'descricao', 'num_doc', 'valor',
        'idcategoria', 'observacao', 'nome_arquivo', 'data_carga'
    ]
    df_final = df[colunas_finais]

    # Excluir dados antigos da tabela
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM fluxo.bbrasil;"))
        conn.commit()
        print("Todos os dados anteriores da tabela fluxo.bbrasil foram excluídos.")

    # Inserir os novos dados
    df_final.to_sql('bbrasil', engine, schema='fluxo', if_exists='append', index=False)
    print(f"{len(df_final)} linhas inseridas com sucesso na tabela fluxo.bbrasil.")

except Exception as e:
    print(f"Erro ao processar o arquivo: {e}")
