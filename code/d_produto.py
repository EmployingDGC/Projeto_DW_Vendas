import utilities as utl
import DW_TOOLS as dwt

from sqlalchemy.types import (
    Integer,
    String,
    DateTime
)


# def get(conn_input):
#     return utl.convert_table_to_dataframe(
#         conn_input=conn_input,
#         schema_name="stage",
#         table_name="STG_PRODUTO",
#         columns=[
#             "id_produto",
#             "cod_barra",
#             "nome_produto",
#             "data_cadastro",
#             "ativo"
#         ]
#     )


def extract_dim_produto(conn_input):
    return dwt.read_table(
        conn=conn_input,
        schema="stage",
        table_name="STG_PRODUTO",
        columns=[
            "id_produto",
            "cod_barra",
            "nome_produto",
            "data_cadastro",
            "ativo"
        ]
    )


def treat(frame):
    columns_rename = {
        "id_produto": "CD_PRODUTO",
        "cod_barra": "CD_BARRAS",
        "nome_produto": "NO_PRODUTO",
        "data_cadastro": "DT_CADASTRO",
        "ativo": "FL_ATIVO"
    }

    order_columns = [
        "SK_PRODUTO",
        "CD_PRODUTO",
        "CD_BARRAS",
        "NO_PRODUTO",
        "DT_CADASTRO",
        "FL_ATIVO"
    ]

    return frame.assign(
        id_produto=lambda df: utl.convert_column_to_int64(df.id_produto, -3),
        cod_barra=lambda df: utl.convert_column_to_int64(df.cod_barra, -3),
        nome_produto=lambda df: utl.convert_column_to_tittle(df.nome_produto),
        data_cadastro=lambda df: utl.convert_column_to_date(df.data_cadastro, "%d%m%Y", "01011900"),
        ativo=lambda df: utl.convert_column_to_int64(df.ativo, -3),
        SK_PRODUTO=lambda df: utl.create_index_dataframe(df, 1)
    ).rename(
        columns=columns_rename
    ).pipe(
        func=utl.insert_default_values_table
    ).filter(
        items=order_columns
    )


def run(conn_input):
    dtypes = {
        "SK_PRODUTO": Integer(),
        "CD_PRODUTO": Integer(),
        "CD_BARRAS": Integer(),
        "NO_PRODUTO": String(),
        "DT_CADASTRO": DateTime(),
        "FL_ATIVO": Integer()
    }

    utl.create_schema(conn_input, "dw")

    extract_dim_produto(conn_input).pipe(
        func=treat
    ).to_sql(
        name="D_PRODUTO",
        con=conn_input,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )
