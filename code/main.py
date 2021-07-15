from time import time

import pandas as pd


# import utilities as utl
# import DW_TOOLS as dwt
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


def run_stg(conn_input):
    stg_cliente.run_stg_cliente(conn_input)
    stg_endereco.run_stg_endereco(conn_input)
    stg_forma_pagamento.run_stg_forma_pagamento(conn_input)
    stg_funcionario.run_stg_funcionario(conn_input)
    stg_item_venda.run_stg_item_venda(conn_input)
    stg_loja.run_stg_loja(conn_input)
    stg_produto.run_stg_produto(conn_input)
    stg_venda.run_stg_venda(conn_input)


def run_dms(conn_output):
    d_categoria.run_dim_categoria(conn_output)
    d_cliente.run_dim_cliente(conn_output)
    d_data.run_dim_data(conn_output)
    d_endereco.run_dim_endereco(conn_output)
    d_funcionario.run_dim_funcionario(conn_output)
    d_loja.run_dim_loja(conn_output)
    d_produto.run_dim_produto(conn_output)
    d_tipo_pagamento.run_dim_tipo_pagamento(conn_output)


def run_fact(conn_output):
    f_venda_produto.run_fact_venda_produto(conn_output)


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

    # run(conn_db)

    f_venda_produto.run_fact_venda_produto(conn_db)

    # print(
    #     dwt.read_table(
    #         conn=conn_db,
    #         schema="dw",
    #         table_name="D_PRODUTO",
    #         where=f"\"CD_PRODUTO\" = 7"
    #     ).iloc[0].SK_PRODUTO
    # )

    print(f"\nFinalizado com sucesso em {round(time() - time_initial)} segundos\n")
