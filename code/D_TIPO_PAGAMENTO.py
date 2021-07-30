import utilities as utl
import DW_TOOLS as dwt

from sqlalchemy.types import (
    Integer,
    String
)


def extract_dim_tipo_pagamento(connection):
    """
    Extrai os dados para fazer a dimensão tipo de pagamento
    :param connection: conexão com o banco de dados das stages
    :return: dataframe com os dados extraidos da stage forma_pagamento
    """

    return dwt.read_table(
        conn=connection,
        schema="stage",
        table_name="STG_FORMA_PAGAMENTO",
        columns=[
            "id_pagamento",
            "nome",
            "descricao"
        ]
    )


def treat_dim_tipo_pagamento(frame):
    """
    Trata os dados para fazer a dimensão tipo_pagamento
    :param frame: dataframe com os dados extraidos
    :return: dataframe com os dados tratados para fazer a dimensão tipo pagamento
    """

    order_columns = [
        "SK_TIPO_PAGAMENTO",
        "CD_TIPO_PAGAMENTO",
        "NO_TIPO_PAGAMENTO",
        "DS_TIPO_PAGAMENTO"
    ]

    return frame.assign(
        CD_TIPO_PAGAMENTO=lambda df: df.id_pagamento,
        NO_TIPO_PAGAMENTO=lambda df: utl.convert_column_to_upper(df.nome),
        DS_TIPO_PAGAMENTO=lambda df: utl.convert_column_to_upper(df.descricao),
        SK_TIPO_PAGAMENTO=lambda df: utl.create_index_dataframe(df, 1)
    ).pipe(
        func=utl.insert_default_values_table
    ).filter(
        items=order_columns
    )


def load_dim_tipo_pagamento(connection):
    """
    Carrega os dados da dimensão tipo_pagamento no banco de daods
    :param connection: conexão com o banco de dados de saída
    :return: None
    """

    dtypes = {
        "SK_TIPO_PAGAMENTO": Integer(),
        "CD_TIPO_PAGAMENTO": Integer(),
        "NO_TIPO_PAGAMENTO": String(),
        "DS_TIPO_PAGAMENTO": String()
    }

    utl.create_schema(connection, "dw")

    extract_dim_tipo_pagamento(connection).pipe(
        func=treat_dim_tipo_pagamento
    ).to_sql(
        name="D_TIPO_PAGAMENTO",
        con=connection,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )
