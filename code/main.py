from time import time

import pandas as pd

import connection as conn

import stages as stg


def run_stg(connection):
    stg.stg_cliente(connection)
    stg.stg_endereco(connection)
    stg.stg_forma_pagamento(connection)
    stg.stg_funcionario(connection)
    stg.stg_item_venda(connection)
    stg.stg_loja(connection)
    stg.stg_produto(connection)
    stg.stg_venda(connection)


def run_dms(connection):
    pass


def run_fact(connection):
    pass


def run(connection):
    run_stg(connection)
    run_dms(connection)
    run_fact(connection)


if __name__ == "__main__":
    time_exec = time()
    time_initial = time()

    pd.set_option("display.max_columns", None)

    db_connection = conn.create_connection_postgre(
        server="10.0.0.105",
        database="projeto_dw_vendas_refatorado",
        username="postgres",
        password="itix.123",
        port=5432
    )

    run(db_connection)

    print(f"\nFinalizado com sucesso em {round(time() - time_initial)} segundos\n")
