from google.cloud import bigquery

table_combustivel = [
    bigquery.SchemaField("estado_sigla", "STRING"),
    bigquery.SchemaField("municipio", "STRING"),
    bigquery.SchemaField("revenda", "STRING"),
    bigquery.SchemaField("produto", "STRING"),
    bigquery.SchemaField("valor_de_venda", "FLOAT64"),
    bigquery.SchemaField("bandeira", "STRING"),
    bigquery.SchemaField("data_da_coleta", "DATE"),
    bigquery.SchemaField("update_date", "TIMESTAMP")
]

municipio_ibge = [
    bigquery.SchemaField("codigo_municipio", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("municipio", "STRING"),
    bigquery.SchemaField("uf", "STRING"),
    bigquery.SchemaField("update_date", "TIMESTAMP")
]