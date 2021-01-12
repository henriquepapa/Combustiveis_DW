import pandas as pd
import os
from datetime import datetime as dt
from datetime import timezone, timedelta
from google.cloud import bigquery
from dotenv import find_dotenv, load_dotenv
from tqdm import tqdm

from utils import utils

load_dotenv(find_dotenv())

# Construct a BigQuery client object.
def carga_ibge():
    client = bigquery.Client()

    tqdm.write('Inserindo na Tabela temporaria!')

    #fuso horario
    diferenca = timedelta(hours=-3)
    fuso_horario = timezone(diferenca)

    table_id = os.getenv('TABLE_ID')

    #Carga_IBGE
    df_ibge = pd.read_excel('Base_CSV/IBGE/RELATORIO_DTB_BRASIL_MUNICIPIO.xls', header=0,
                            usecols=['Nome_UF', 'Nome_Município', 'Código Município Completo'])

    #Nomalizacao das colunas
    df_ibge.columns = utils.normalize_columns(list(df_ibge.columns))
    df_ibge['nome_uf'] = df_ibge['nome_uf'].apply(utils.convert_state)
    df_ibge['nome_municipio'] = df_ibge['nome_municipio'].apply(utils.normalize_city)
    df_ibge.rename(columns={'nome_uf': 'uf',
                            'codigo_municipio_completo': 'codigo_municipio', 'nome_municipio': 'municipio'},
                   inplace=True
                   )
    df_ibge['update_date'] = dt.now().astimezone(fuso_horario)

    table_create_id_tmp = table_id + ".municipio_ibge_tmp"
    table_create_id = table_id + ".municipio_ibge"

    job = client.load_table_from_dataframe(
        df_ibge, table_create_id_tmp
    )
    job.result()

    tqdm.write('Mesclando os dados com produção!')
    sql = """
    MERGE {} A
    USING {} T
    ON T.codigo_municipio = A.codigo_municipio
    WHEN MATCHED THEN
      UPDATE SET
            A.uf = T.uf,
            A.municipio = T.municipio,
            A.update_date = T.update_date
    WHEN NOT MATCHED THEN
        INSERT (
            uf, codigo_municipio, municipio,update_date)
        VALUES (
            T.uf, T.codigo_municipio, T.municipio,T.update_date
        )
    """.format(
        table_create_id,
        table_create_id_tmp
    )

    job = client.query(sql)  # API request.
    job.result()

    tqdm.write('Limpando tabela Temporaria!')
    sql = """
                TRUNCATE TABLE {}
                """.format(
                    table_create_id_tmp
                )
    job = client.query(sql)  # API request.
    job.result()

