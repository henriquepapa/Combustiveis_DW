from google.cloud.exceptions import NotFound
from google.cloud import bigquery
import os
from dotenv import find_dotenv, load_dotenv

from schema import schema_table
import time

load_dotenv(find_dotenv())


def main():
    # Construct a BigQuery client object.
    client = bigquery.Client()

    table_id = os.getenv('TABLE_ID')

    list_table = ["combustivel_etanol", "combustivel_gasolina", "combustivel_diesel_s10", "combustivel_diesel",
                  "combustivel_gnv", "combustivel_glp"]

    for table_name in list_table:
        table_create_id = table_id + '.' + table_name
        try:
            client.get_table(table_create_id) # se false apenas cria
            sql = """
            DROP TABLE {}
            """.format(
                table_create_id
            )
            job = client.query(sql)  # API request.
            job.result()

            table = bigquery.Table(table_create_id, schema=schema_table.table_combustivel)
            table = client.create_table(table)
            print(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )

        except NotFound:
            table = bigquery.Table(table_create_id, schema=schema_table.table_combustivel)
            table = client.create_table(table)
            print(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )
        time.sleep(2)

    list_table = ["municipio_ibge", "municipio_ibge_tmp"]

    for table_name in list_table:
        # create ibge
        table_create_id = table_id + "." + table_name
        try:
            client.get_table(table_create_id)
            sql = """
                        DROP TABLE {}
                        """.format(
                table_create_id
            )
            job = client.query(sql)  # API request.
            job.result()

            table = bigquery.Table(table_create_id, schema=schema_table.municipio_ibge)
            table = client.create_table(table)
            print(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )
        except NotFound:
            table = bigquery.Table(table_create_id, schema=schema_table.municipio_ibge)
            table = client.create_table(table)
            print(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )


if __name__ == '__main__':
    main()
