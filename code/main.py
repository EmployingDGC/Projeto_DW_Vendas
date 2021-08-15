from time import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd


import connection as conn
import stages as stg
import utilities as utl

import D_CLIENTE
import D_DATA
import D_FUNCIONARIO
import D_LOJA
import D_PRODUTO
import D_TIPO_PAGAMENTO

import F_VENDA_PRODUTO


def run_stg(connection):
    utl.create_schema(connection, "stage")

    stg.run_stg_cliente(connection)
    stg.run_stg_forma_pagamento(connection)
    stg.run_stg_funcionario(connection)
    stg.run_stg_item_venda(connection)
    stg.run_stg_loja(connection)
    stg.run_stg_produto(connection)
    stg.run_stg_venda(connection)


def run_dms(connection):
    utl.create_schema(connection, "dw")

    D_CLIENTE.run_dim_cliente(connection)
    D_DATA.run_dim_data(connection)
    D_FUNCIONARIO.run_dim_funcionario(connection)
    D_LOJA.run_dim_loja(connection)
    D_PRODUTO.run_dim_produto(connection)
    D_TIPO_PAGAMENTO.run_dim_tipo_pagamento(connection)


def run_fact(connection):
    F_VENDA_PRODUTO.run_fact_venda_produto(connection)


def run(connection):
    run_stg(connection)
    run_dms(connection)
    run_fact(connection)


if __name__ == "__main__":
    pass

time_exec = time()
time_initial = time()

pd.set_option("display.max_columns", None)

conn_db = conn.create_connection_postgre(
    server="10.0.0.105",
    database="projeto_dw_vendas",
    username="postgres",
    password="itix.123",
    port=5432
)

with DAG(
    dag_id="vendas_dag",
    start_date=datetime(day=16, month=2, year=2021),
    schedule_interval="@daily",
    catchup=False
) as dag:
    vertex_schema_stage = PythonOperator(
        task_id="vertex_schema_stage",
        python_callable=utl.create_schema,
        op_kwargs={
            "database": conn_db,
            "schema_name": "stage"
        },
        dag=dag
    )

    vertex_schema_dw = PythonOperator(
        task_id="vertex_schema_dw",
        python_callable=utl.create_schema,
        op_kwargs={
            "database": conn_db,
            "schema_name": "dw"
        },
        dag=dag
    )

    vertex_stg_cliente = PythonOperator(
        task_id="vertex_stg_cliente",
        python_callable=stg.run_stg_cliente,
        op_kwargs={"connection": conn_db},
        dag=dag
    )

    vertex_stg_endereco = PythonOperator(
        task_id="vertex_stg_endereco",
        python_callable=stg.run_stg_endereco,
        op_kwargs={"connection": conn_db},
        dag=dag
    )

    vertex_stg_forma_pagamento = PythonOperator(
        task_id="vertex_stg_forma_pagamento",
        python_callable=stg.run_stg_forma_pagamento,
        op_kwargs={"connection": conn_db},
        dag=dag
    )

    vertex_stg_funcionario = PythonOperator(
        task_id="vertex_stg_funcionario",
        python_callable=stg.run_stg_funcionario,
        op_kwargs={"connection": conn_db},
        dag=dag
    )

    vertex_stg_item_venda = PythonOperator(
        task_id="vertex_stg_item_venda",
        python_callable=stg.run_stg_item_venda,
        op_kwargs={"connection": conn_db},
        dag=dag
    )

    vertex_stg_loja = PythonOperator(
        task_id="vertex_stg_loja",
        python_callable=stg.run_stg_loja,
        op_kwargs={"connection": conn_db},
        dag=dag
    )

    vertex_stg_produto = PythonOperator(
        task_id="vertex_stg_produto",
        python_callable=stg.run_stg_produto,
        op_kwargs={"connection": conn_db},
        dag=dag
    )

    vertex_stg_venda = PythonOperator(
        task_id="vertex_stg_venda",
        python_callable=stg.run_stg_venda,
        op_kwargs={"connection": conn_db},
        dag=dag
    )

    vertex_d_cliente = PythonOperator(
        task_id="vertex_d_cliente",
        python_callable=D_CLIENTE.run_dim_cliente,
        op_kwargs={"connection": conn_db},
        dag=dag
    )

    vertex_d_data = PythonOperator(
        task_id="vertex_d_data",
        python_callable=D_DATA.run_dim_data,
        op_kwargs={"connection": conn_db},
        dag=dag
    )

    vertex_d_funcionario = PythonOperator(
        task_id="vertex_d_funcionario",
        python_callable=D_FUNCIONARIO.run_dim_funcionario,
        op_kwargs={"connection": conn_db},
        dag=dag
    )

    vertex_d_loja = PythonOperator(
        task_id="vertex_d_loja",
        python_callable=D_LOJA.run_dim_loja,
        op_kwargs={"connection": conn_db},
        dag=dag
    )

    vertex_d_produto = PythonOperator(
        task_id="vertex_d_produto",
        python_callable=D_PRODUTO.run_dim_produto,
        op_kwargs={"connection": conn_db},
        dag=dag
    )

    vertex_d_tipo_pagamento = PythonOperator(
        task_id="vertex_d_tipo_pagamento",
        python_callable=D_TIPO_PAGAMENTO.run_dim_tipo_pagamento,
        op_kwargs={"connection": conn_db},
        dag=dag
    )

    vertex_f_venda_produto = PythonOperator(
        task_id="vertex_f_venda_produto",
        python_callable=F_VENDA_PRODUTO.run_fact_venda_produto,
        op_kwargs={"connection": conn_db},
        dag=dag
    )

    vertex_schema_dw >> [
        vertex_d_cliente,
        vertex_d_data,
        vertex_d_funcionario,
        vertex_d_loja,
        vertex_d_produto,
        vertex_d_tipo_pagamento
    ]

    vertex_schema_stage >> [
        vertex_stg_cliente,
        vertex_stg_endereco,
        vertex_stg_forma_pagamento,
        vertex_stg_funcionario,
        vertex_stg_item_venda,
        vertex_stg_loja,
        vertex_stg_produto,
        vertex_stg_venda
    ]

    [
        vertex_stg_cliente,
        vertex_stg_endereco
    ] >> vertex_d_cliente

    [
        vertex_schema_dw,
    ] >> vertex_d_data

    [
        vertex_stg_funcionario
    ] >> vertex_d_funcionario

    [
        vertex_stg_loja,
        vertex_stg_endereco
    ] >> vertex_d_loja

    [
        vertex_stg_produto
    ] >> vertex_d_produto

    [
        vertex_stg_forma_pagamento
    ] >> vertex_d_tipo_pagamento

    [
        vertex_stg_venda,
        vertex_stg_item_venda,
        vertex_d_cliente,
        vertex_d_data,
        vertex_d_funcionario,
        vertex_d_loja,
        vertex_d_produto,
        vertex_d_tipo_pagamento
    ] >> vertex_f_venda_produto

print(f"\nFinalizado com sucesso em {round(time() - time_initial)} segundos\n")
