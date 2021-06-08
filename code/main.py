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
import d_funcionario
import d_tipo_pagamento
import d_produto
import d_loja
import d_endereco


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

    # criaçao das stages

    # stg_cliente.run(conn_db)
    # stg_endereco.run(conn_db)
    # stg_forma_pagamento.run(conn_db)
    # stg_funcionario.run(conn_db)
    # stg_item_venda.run(conn_db)
    # stg_loja.run(conn_db)
    # stg_produto.run(conn_db)
    # stg_venda.run(conn_db)

    # criação das dimensões

    # d_categoria.run(conn_db)
    # d_cliente.run(conn_db)
    # d_funcionario.run(conn_db)
    # d_tipo_pagamento.run(conn_db)
    # d_produto.run(conn_db)
    # d_loja.run(conn_db)
    d_endereco.run(conn_db)

    print(f"\nFinalizado com sucesso em {round(time() - time_initial)} segundos\n")
