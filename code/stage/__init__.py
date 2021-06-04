from sqlalchemy.engine.mock import MockConnection

from stage import (
    cliente,
    endereco,
    forma_pagamento,
    funcionario,
    item_venda,
    loja,
    produto,
    venda
)

import utilities as utl
import default as dflt


def run(conn_input: MockConnection) -> None:
    utl.create_schema(
        database=conn_input,
        schema_name=dflt.schema.stage
    )

    cliente.create(conn_input)
    endereco.create(conn_input)
    forma_pagamento.create(conn_input)
    funcionario.create(conn_input)
    item_venda.create(conn_input)
    loja.create(conn_input)
    produto.create(conn_input)
    venda.create(conn_input)
