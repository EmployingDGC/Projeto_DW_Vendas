import utilities as utl
import DW_TOOLS as dwt

from sqlalchemy.types import (
    Integer,
    String
)


# def get(conn_input):
#     return utl.convert_table_to_dataframe(
#         conn_input=conn_input,
#         schema_name="stage",
#         table_name="STG_FORMA_PAGAMENTO",
#         columns=[
#             "id_pagamento",
#             "nome",
#             "descricao"
#         ]
#     )


def extract_dim_tipo_pagamento(conn_input):
    return dwt.read_table(
        conn=conn_input,
        schema="stage",
        table_name="STG_FORMA_PAGAMENTO",
        columns=[
            "id_pagamento",
            "nome",
            "descricao"
        ]
    )


def treat_dim_tipo_pagamento(frame):
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


def run_dim_tipo_pagamento(conn_input):
    dtypes = {
        "SK_TIPO_PAGAMENTO": Integer(),
        "CD_TIPO_PAGAMENTO": Integer(),
        "NO_TIPO_PAGAMENTO": String(),
        "DS_TIPO_PAGAMENTO": String()
    }

    utl.create_schema(conn_input, "dw")

    extract_dim_tipo_pagamento(conn_input).pipe(
        func=treat_dim_tipo_pagamento
    ).to_sql(
        name="D_TIPO_PAGAMENTO",
        con=conn_input,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )
