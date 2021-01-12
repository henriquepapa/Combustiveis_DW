import pandas as pd
import os
from datetime import datetime as dt
from datetime import timezone, timedelta
from google.cloud import bigquery
from dotenv import find_dotenv, load_dotenv
from tqdm import tqdm
import glob

from utils import utils

load_dotenv(find_dotenv())

# Construct a BigQuery client object.
def carga_glp():
    client = bigquery.Client()

    tqdm.write('Inserindo na Tabela')

    #fuso horario
    diferenca = timedelta(hours=-3)
    fuso_horario = timezone(diferenca)

    table_id = os.getenv('TABLE_ID')

    path = 'Base_CSV/GLP'
    os.chdir(path)

    df = pd.DataFrame(columns=['Estado - Sigla', 'Município', 'Revenda', 'Produto', 'Data da Coleta', 'Valor de Venda',
                               'Bandeira'])
    for file in glob.glob('*.csv'):
        df = df.append(pd.read_csv(file, sep='\t', header=0, encoding='UTF-16',
                                   usecols=['Estado - Sigla', 'Município', 'Revenda', 'Produto', 'Data da Coleta',
                                            'Valor de Venda',
                                            'Bandeira']), ignore_index=True)

    df.columns = utils.normalize_columns(list(df.columns))
    df['data_da_coleta'] = pd.to_datetime(df['data_da_coleta'], format='%d/%m/%Y')
    df['valor_de_venda'] = [str(x).replace(',', '.') for x in df['valor_de_venda']]
    df['valor_de_venda'] = df['valor_de_venda'].astype(float)
    df['update_date'] = dt.now().astimezone(fuso_horario)

    table_create_id = table_id + ".combustivel_glp"

    job = client.load_table_from_dataframe(
        df, table_create_id
    )
    job.result()