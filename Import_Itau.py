import pandas as pd
import os
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import shutil
import re

# ==================== 1. Configurações iniciais ====================
# Carregar variáveis do .env
load_dotenv()

# Obter dados de conexão
db_user = os.getenv('LOCAL_USER')
db_pass = os.getenv('LOCAL_PASS')
db_host = os.getenv('LOCAL_HOST')
db_port = os.getenv('LOCAL_PORT')
db_name = os.getenv('LOCAL_DB')

# Criar engine de conexão
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

# Caminho da pasta dos arquivos
folder_path = r'D:\01-Estudos\00-BaseDados\01-FluxoCaixa\01-Itau\01-Input'
pasta_processados = os.path.join(folder_path, '01-Processados')
os.makedirs(pasta_processados, exist_ok=True)

# ==================== 2. Função auxiliar ====================
def aplicar_nomes_colunas(df, novos_nomes, arquivo):
    if len(novos_nomes) == len(df.columns):
        df.columns = novos_nomes
    else:
        print(f"Erro: A quantidade de novos nomes não corresponde ao número de colunas em {arquivo}.")
    return df

# ==================== 3. Função principal: excluir e inserir ====================
def excluir_e_inserir(df, engine):
    with engine.connect() as conn:
        check_table_query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'fluxo'
                AND table_name = 'itau'
            );
        """)
        table_exists = conn.execute(check_table_query).scalar()

        if table_exists:
            query = """
                SELECT ano_mes
                FROM fluxo.itau
                GROUP BY ano_mes;
            """
            arquivos_importados = pd.read_sql(query, engine)
        else:
            arquivos_importados = pd.DataFrame(columns=['ano_mes'])

    # Aqui é o ajuste principal: comparar ano_mes com ano_mes
    arquivos_para_atualizar = df[df['ano_mes'].isin(arquivos_importados['ano_mes'])]

    num_excluidos = 0
    if not arquivos_para_atualizar.empty:
        with engine.connect() as conn:
            for ano_mes in arquivos_para_atualizar['ano_mes'].unique():
                delete_query = text("""
                    DELETE FROM fluxo.itau 
                    WHERE ano_mes = :ano_mes
                """)
                result = conn.execute(delete_query, {"ano_mes": ano_mes})
                conn.commit()
                num_excluidos += result.rowcount

    num_incluidos = len(df)

    df.to_sql('itau', engine, schema='fluxo', if_exists='append', index=False)

    print(f"\nProcessamento concluído.")
    print(f"Arquivos processados: {df['nome_arquivo'].nunique()}")
    print(f"Linhas excluídas: {num_excluidos}")
    print(f"Linhas inseridas: {num_incluidos}")


# ==================== 4. Início do processamento ====================
arquivos = [f for f in os.listdir(folder_path) if f.endswith('.xls') or f.endswith('.xlsx')]

if not arquivos:
    print(f"Nenhum arquivo .xls ou .xlsx encontrado na pasta: {folder_path}")
    exit()

df_list = []

nomes_colunas = [
    'data_lanc', 'lancamento', 'ag_origem', 'valor', 'saldos',
    'idcategoria', 'observacao'
]

for arquivo in arquivos:
    file_path = os.path.join(folder_path, arquivo)
    try:
        df = pd.read_excel(file_path, header=None)
        header_row = df[df.iloc[:, 0].astype(str).str.contains('data', case=False, na=False)].index[0]
        df = pd.read_excel(file_path, header=header_row)
        df = aplicar_nomes_colunas(df, nomes_colunas, arquivo)
        df['nome_arquivo'] = re.sub(r'\.[^.]+$', '', arquivo.strip())  # Remove extensão
        df_list.append(df)

        destino = os.path.join(pasta_processados, arquivo)
        shutil.move(file_path, destino)
        print(f"Arquivo movido para pasta de processados: {arquivo}")

    except Exception as e:
        print(f"Erro ao processar {arquivo}: {e}")

# ==================== 5. Consolidação e gravação ====================
if df_list:
    df_combined = pd.concat(df_list, ignore_index=True)
    df_combined['data_lanc'] = pd.to_datetime(df_combined['data_lanc'], errors='coerce', dayfirst=True)
    df_combined = df_combined.sort_values(by='data_lanc')
    df_combined['data_carga'] = pd.to_datetime('today').normalize()

    # Criação da coluna ano_mes com base nos 7 primeiros caracteres do nome do arquivo
    df_combined['ano_mes'] = df_combined['nome_arquivo'].str[:7].str.replace(r'(\d{4})(\d{2})', r'\1-\2', regex=True)

    colunas_finais = [
        'data_lanc', 'lancamento', 'ag_origem', 'valor', 'saldos',
        'idcategoria', 'observacao', 'nome_arquivo', 'ano_mes', 'data_carga'
    ]
    df_filtrado = df_combined[colunas_finais]

    excluir_e_inserir(df_filtrado, engine)
else:
    print("Nenhum arquivo válido foi processado.")

