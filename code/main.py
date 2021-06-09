from time import time

import pandas as pd


# import utilities as utl
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
import d_turno


def run_stg(conn_input):
    stg_cliente.run(conn_input)
    stg_endereco.run(conn_input)
    stg_forma_pagamento.run(conn_input)
    stg_funcionario.run(conn_input)
    stg_item_venda.run(conn_input)
    stg_loja.run(conn_input)
    stg_produto.run(conn_input)
    stg_venda.run(conn_input)


def run_dms(conn_output):
    d_categoria.run(conn_output)
    d_cliente.run(conn_output)
    d_data.run(conn_output)
    d_endereco.run(conn_output)
    d_funcionario.run(conn_output)
    d_loja.run(conn_output)
    d_produto.run(conn_output)
    d_tipo_pagamento.run(conn_output)
    d_turno.run(conn_output)


def run(connection):
    run_stg(connection)
    run_dms(connection)


if __name__ == "__main__":
    time_exec = time()
    time_initial = time()

    pd.set_option("display.max_columns", None)

    conn_db = conn.create_connection_postgre(
        server="10.0.0.102",
        database="projeto_dw_vendas",
        username="postgres",
        password="itix.123",
        port=5432
    )

    run(conn_db)

    print(f"\nFinalizado com sucesso em {round(time() - time_initial)} segundos\n")
