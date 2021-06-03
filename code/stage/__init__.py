from sqlalchemy.engine.mock import MockConnection

import stage.cliente as cliente
import stage.endereco as endereco
import stage.forma_pagamento as forma_pagamento
import stage.funcionario as funcionario
import stage.item_venda as item_venda
import stage.loja as loja
import stage.produto as produto
import stage.venda as venda

import utilities as utl
import default as dflt


def run(conn_input: MockConnection) -> None:
    utl.create_schema(
        database=conn_input,
        schema_name=dflt.Schemas.stage
    )

    cliente.create(conn_input)
    endereco.create(conn_input)
    forma_pagamento.create(conn_input)
    funcionario.create(conn_input)
    item_venda.create(conn_input)
    loja.create(conn_input)
    produto.create(conn_input)
    venda.create(conn_input)
