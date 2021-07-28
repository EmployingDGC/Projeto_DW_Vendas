from time import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd


import connection as conn

import stg_cliente
import stg_endereco
import stg_forma_pagamento
import stg_funcionario
import stg_item_venda
import stg_loja
import stg_produto
import stg_venda

import d_categoria
import d_cliente
import d_data
import d_endereco
import d_funcionario
import d_loja
import d_produto
import d_tipo_pagamento

import f_venda_produto


def run_stg(connection):
    stg_cliente.load_stg_cliente(connection)
    stg_endereco.load_stg_endereco(connection)
    stg_forma_pagamento.load_stg_forma_pagamento(connection)
    stg_funcionario.load_stg_funcionario(connection)
    stg_item_venda.load_stg_item_venda(connection)
    stg_loja.load_stg_loja(connection)
    stg_produto.load_stg_produto(connection)
    stg_venda.load_stg_venda(connection)


def run_dms(connection):
    d_categoria.load_dim_categoria(connection)
    d_cliente.load_dim_cliente(connection)
    d_data.load_dim_data(connection)
    d_endereco.load_dim_endereco(connection)
    d_funcionario.load_dim_funcionario(connection)
    d_loja.load_dim_loja(connection)
    d_produto.load_dim_produto(connection)
    d_tipo_pagamento.load_dim_tipo_pagamento(connection)


def run_fact(connection):
    f_venda_produto.load_fact_venda_produto(connection)


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
            python_callable=stg_cliente.load_stg_cliente,
            op_kwargs={"connection": conn_db}
        )

        vertex_stg_endereco = PythonOperator(
            task_id="vertex_stg_endereco",
            python_callable=stg_endereco.load_stg_endereco,
            op_kwargs={"connection": conn_db}
        )

        vertex_stg_forma_pagamento = PythonOperator(
            task_id="vertex_stg_forma_pagamento",
            python_callable=stg_forma_pagamento.load_stg_forma_pagamento,
            op_kwargs={"connection": conn_db}
        )

        vertex_stg_funcionario = PythonOperator(
            task_id="vertex_stg_funcionario",
            python_callable=stg_funcionario.load_stg_funcionario,
            op_kwargs={"connection": conn_db}
        )

        vertex_stg_item_venda = PythonOperator(
            task_id="vertex_stg_item_venda",
            python_callable=stg_item_venda.load_stg_item_venda,
            op_kwargs={"connection": conn_db}
        )

        vertex_stg_loja = PythonOperator(
            task_id="vertex_stg_loja",
            python_callable=stg_loja.load_stg_loja,
            op_kwargs={"connection": conn_db}
        )

        vertex_stg_produto = PythonOperator(
            task_id="vertex_stg_produto",
            python_callable=stg_produto.load_stg_produto,
            op_kwargs={"connection": conn_db}
        )

        vertex_stg_venda = PythonOperator(
            task_id="vertex_stg_venda",
            python_callable=stg_venda.load_stg_venda,
            op_kwargs={"connection": conn_db}
        )

        vertex_d_categoria = PythonOperator(
            task_id="vertex_d_categoria",
            python_callable=d_categoria.load_dim_categoria,
            op_kwargs={"connection": conn_db}
        )

        vertex_d_cliente = PythonOperator(
            task_id="vertex_d_cliente",
            python_callable=d_cliente.load_dim_cliente,
            op_kwargs={"connection": conn_db}
        )

        vertex_d_data = PythonOperator(
            task_id="vertex_d_data",
            python_callable=d_data.load_dim_data,
            op_kwargs={"connection": conn_db}
        )

        vertex_d_endereco = PythonOperator(
            task_id="vertex_d_endereco",
            python_callable=d_endereco.load_dim_endereco,
            op_kwargs={"connection": conn_db}
        )

        vertex_d_funcionario = PythonOperator(
            task_id="vertex_d_funcionario",
            python_callable=d_funcionario.load_dim_funcionario,
            op_kwargs={"connection": conn_db}
        )

        vertex_d_loja = PythonOperator(
            task_id="vertex_d_loja",
            python_callable=d_loja.load_dim_loja,
            op_kwargs={"connection": conn_db}
        )

        vertex_d_produto = PythonOperator(
            task_id="vertex_d_produto",
            python_callable=d_produto.load_dim_produto,
            op_kwargs={"connection": conn_db}
        )

        vertex_d_tipo_pagamento = PythonOperator(
            task_id="vertex_d_tipo_pagamento",
            python_callable=d_tipo_pagamento.load_dim_tipo_pagamento,
            op_kwargs={"connection": conn_db}
        )

        vertex_f_venda_produto = PythonOperator(
            task_id="vertex_f_venda_produto",
            python_callable=f_venda_produto.load_fact_venda_produto,
            op_kwargs={"connection": conn_db}
        )

        [
            vertex_stg_cliente,
            vertex_stg_endereco,
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
            vertex_d_endereco,
            vertex_d_funcionario,
            vertex_d_loja,
            vertex_d_produto,
            vertex_d_tipo_pagamento,
        ] >> vertex_f_venda_produto

    print(f"\nFinalizado com sucesso em {round(time() - time_initial)} segundos\n")
