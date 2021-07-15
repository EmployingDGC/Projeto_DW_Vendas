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

    # run(conn_db)

    stg_loja.load_stg_loja(conn_db)

    # print(
    #     dwt.read_table(
    #         conn=conn_db,
    #         schema="dw",
    #         table_name="D_PRODUTO",
    #         where=f"\"CD_PRODUTO\" = 7"
    #     ).iloc[0].SK_PRODUTO
    # )

    print(f"\nFinalizado com sucesso em {round(time() - time_initial)} segundos\n")
