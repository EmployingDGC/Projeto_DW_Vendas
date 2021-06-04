from sqlalchemy.engine.mock import MockConnection

from dimension import (
    categoria,
    cliente,
    data,
    endereco,
    funcionario,
    loja,
    pagamento,
    produto
)

import utilities as utl
import default as dflt


def run(conn_input: MockConnection) -> None:
    utl.create_schema(
        database=conn_input,
        schema_name=dflt.Schema.dw
    )

    categoria.create(conn_input)
