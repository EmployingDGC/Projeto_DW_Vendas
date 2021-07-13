import utilities as utl
import DW_TOOLS as dwt

from sqlalchemy.types import (
    Integer,
    String,
    BigInteger,
    DateTime
)


# def get(conn_input):
#     return utl.convert_table_to_dataframe(
#         conn_input=conn_input,
#         schema_name="stage",
#         table_name="STG_LOJA",
#         columns=[
#             "id_loja",
#             "id_endereco",
#             "cnpj",
#             "nome_loja",
#             "razao_social",
#             "data_inicial",
#             "data_final",
#             "ativo"
#         ]
#     )


def extract_dim_loja(conn_input):
    return dwt.read_table(
        conn=conn_input,
        schema="stage",
        table_name="STG_LOJA",
        columns=[
            "id_loja",
            "id_endereco",
            "cnpj",
            "nome_loja",
            "razao_social",
            "data_inicial",
            "data_final",
            "ativo"
        ]
    )


def treat(frame):
    columns_rename = {
        "id_loja": "CD_LOJA",
        "id_endereco": "CD_ENDERECO",
        "cnpj": "CD_CNPJ",
        "nome_loja": "NO_LOJA",
        "razao_social": "NO_RAZAO_SOCIAL",
        "data_inicial": "DT_INICIAL",
        "data_final": "DT_FINAL",
        "ativo": "FL_ATIVO"
    }

    order_columns = [
        "SK_LOJA",
        "CD_LOJA",
        "CD_ENDERECO",
        "CD_CNPJ",
        "DS_CNPJ",
        "NO_LOJA",
        "NO_RAZAO_SOCIAL",
        "DT_INICIAL",
        "DT_FINAL",
        "FL_ATIVO"
    ]

    return frame.assign(
        cnpj=lambda df: utl.convert_column_cnpj_to_int64(df.cnpj, -3),
        nome_loja=lambda df: utl.convert_column_to_tittle(df.nome_loja),
        razao_social=lambda df: utl.convert_column_to_tittle(df.razao_social),
        DS_CNPJ=lambda df: utl.convert_int_cnpj_to_format_cnpj(df.cnpj),
        SK_LOJA=lambda df: utl.create_index_dataframe(df, 1)
    ).rename(
        columns=columns_rename
    ).assign(
        DT_INICIAL=lambda df: df.DT_INICIAL.astype("datetime64[ns]"),
        DT_FINAL=lambda df: df.DT_FINAL.astype("datetime64[ns]")
    ).pipe(
        func=utl.insert_default_values_table
    ).filter(
        items=order_columns
    )


def run(conn_input):
    dtypes = {
        "SK_LOJA": Integer(),
        "CD_LOJA": Integer(),
        "CD_ENDERECO": Integer(),
        "CD_CNPJ": BigInteger(),
        "DS_CNPJ": String(),
        "NO_LOJA": String(),
        "NO_RAZAO_SOCIAL": String(),
        "DT_INICIAL": DateTime(),
        "DT_FINAL": DateTime(),
        "FL_ATIVO": Integer()
    }

    utl.create_schema(conn_input, "dw")

    extract_dim_loja(conn_input).pipe(
        func=treat
    ).to_sql(
        name="D_LOJA",
        con=conn_input,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )
