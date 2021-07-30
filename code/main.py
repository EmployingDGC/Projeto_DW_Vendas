from time import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd


import connection as conn

import stages as stg

import STG_LOJA
import STG_PRODUTO

import D_CATEGORIA
import D_CLIENTE
import D_DATA
import D_FUNCIONARIO
import D_LOJA
import D_PRODUTO
import D_TIPO_PAGAMENTO

import F_VENDA_PRODUTO


def run_stg(connection):
    stg.load_stg_cliente(connection)
    stg.load_stg_forma_pagamento(connection)
    stg.load_stg_funcionario(connection)
    stg.load_stg_item_venda(connection)
    STG_LOJA.load_stg_loja(connection)
    STG_PRODUTO.load_stg_produto(connection)
    stg.load_stg_venda(connection)


def run_dms(connection):
    D_CATEGORIA.load_dim_categoria(connection)
    D_CLIENTE.load_dim_cliente(connection)
    D_DATA.load_dim_data(connection)
    D_FUNCIONARIO.load_dim_funcionario(connection)
    D_LOJA.load_dim_loja(connection)
    D_PRODUTO.load_dim_produto(connection)
    D_TIPO_PAGAMENTO.load_dim_tipo_pagamento(connection)


def run_fact(connection):
    F_VENDA_PRODUTO.load_fact_venda_produto(connection)


def run(connection):
    run_stg(connection)
    run_dms(connection)
    run_fact(connection)


if __name__ == "__main__":
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
        vertex_stg_cliente = PythonOperator(
            task_id="vertex_stg_cliente",
            python_callable=stg.load_stg_cliente,
            op_kwargs={"connection": conn_db}
        )

        vertex_stg_forma_pagamento = PythonOperator(
            task_id="vertex_stg_forma_pagamento",
            python_callable=stg.load_stg_forma_pagamento,
            op_kwargs={"connection": conn_db}
        )

        vertex_stg_funcionario = PythonOperator(
            task_id="vertex_stg_funcionario",
            python_callable=stg.load_stg_funcionario,
            op_kwargs={"connection": conn_db}
        )

        vertex_stg_item_venda = PythonOperator(
            task_id="vertex_stg_item_venda",
            python_callable=stg.load_stg_item_venda,
            op_kwargs={"connection": conn_db}
        )

        vertex_stg_loja = PythonOperator(
            task_id="vertex_stg_loja",
            python_callable=STG_LOJA.load_stg_loja,
            op_kwargs={"connection": conn_db}
        )

        vertex_stg_produto = PythonOperator(
            task_id="vertex_stg_produto",
            python_callable=STG_PRODUTO.load_stg_produto,
            op_kwargs={"connection": conn_db}
        )

        vertex_stg_venda = PythonOperator(
            task_id="vertex_stg_venda",
            python_callable=stg.load_stg_venda,
            op_kwargs={"connection": conn_db}
        )

        vertex_d_categoria = PythonOperator(
            task_id="vertex_d_categoria",
            python_callable=D_CATEGORIA.load_dim_categoria,
            op_kwargs={"connection": conn_db}
        )

        vertex_d_cliente = PythonOperator(
            task_id="vertex_d_cliente",
            python_callable=D_CLIENTE.load_dim_cliente,
            op_kwargs={"connection": conn_db}
        )

        vertex_d_data = PythonOperator(
            task_id="vertex_d_data",
            python_callable=D_DATA.load_dim_data,
            op_kwargs={"connection": conn_db}
        )

        vertex_d_funcionario = PythonOperator(
            task_id="vertex_d_funcionario",
            python_callable=D_FUNCIONARIO.load_dim_funcionario,
            op_kwargs={"connection": conn_db}
        )

        vertex_d_loja = PythonOperator(
            task_id="vertex_d_loja",
            python_callable=D_LOJA.load_dim_loja,
            op_kwargs={"connection": conn_db}
        )

        vertex_d_produto = PythonOperator(
            task_id="vertex_d_produto",
            python_callable=D_PRODUTO.load_dim_produto,
            op_kwargs={"connection": conn_db}
        )

        vertex_d_tipo_pagamento = PythonOperator(
            task_id="vertex_d_tipo_pagamento",
            python_callable=D_TIPO_PAGAMENTO.load_dim_tipo_pagamento,
            op_kwargs={"connection": conn_db}
        )

        vertex_f_venda_produto = PythonOperator(
            task_id="vertex_f_venda_produto",
            python_callable=F_VENDA_PRODUTO.load_fact_venda_produto,
            op_kwargs={"connection": conn_db}
        )

        [
            vertex_stg_cliente,
            vertex_stg_forma_pagamento,
            vertex_stg_funcionario,
            vertex_stg_item_venda,
            vertex_stg_loja,
            vertex_stg_produto,
            vertex_stg_venda
        ] >> [
            vertex_d_categoria,
            vertex_d_cliente,
            vertex_d_data,
            vertex_d_funcionario,
            vertex_d_loja,
            vertex_d_produto,
            vertex_d_tipo_pagamento,
        ] >> vertex_f_venda_produto

    print(f"\nFinalizado com sucesso em {round(time() - time_initial)} segundos\n")
