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
#         table_name="STG_ENDERECO",
#         columns=[
#             "id_endereco",
#             "estado",
#             "cidade",
#             "bairro",
#             "rua"
#         ]
#     )


def extract_dim_endereco(conn_input):
    return dwt.read_table(
        conn=conn_input,
        schema="stage",
        table_name="STG_ENDERECO",
        columns=[
            "id_endereco",
            "estado",
            "cidade",
            "bairro",
            "rua"
        ]
    )


def treat(frame):
    columns_rename = {
        "id_endereco": "CD_ENDERECO",
        "estado": "NO_ESTADO",
        "cidade": "NO_CIDADE",
        "bairro": "NO_BAIRRO",
        "rua": "NO_RUA"
    }

    order_columns = [
        "SK_ENDERECO",
        "CD_ENDERECO",
        "NO_ESTADO",
        "NO_CIDADE",
        "NO_BAIRRO",
        "NO_RUA"
    ]

    return frame.drop_duplicates(
        subset=[k for k in frame.keys()]
    ).assign(
        id_endereco=lambda df: utl.convert_column_to_int64(df.id_endereco, -3),
        estado=lambda df: utl.convert_column_to_tittle(df.estado),
        cidade=lambda df: utl.convert_column_to_tittle(df.cidade),
        bairro=lambda df: utl.convert_column_to_tittle(df.bairro),
        rua=lambda df: utl.convert_column_to_tittle(df.rua),
        SK_ENDERECO=lambda df: utl.create_index_dataframe(df, 1)
    ).rename(
        columns=columns_rename
    ).pipe(
        func=utl.insert_default_values_table
    ).filter(
        items=order_columns
    )


def run(conn_input):
    dtypes = {
        "SK_ENDERECO": Integer(),
        "CD_ENDERECO": Integer(),
        "NO_ESTADO": String(),
        "NO_CIDADE": String(),
        "NO_BAIRRO": String(),
        "NO_RUA": String()
    }

    utl.create_schema(conn_input, "dw")

    extract_dim_endereco(conn_input).pipe(
        func=treat
    ).to_sql(
        name="D_ENDERECO",
        con=conn_input,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )
