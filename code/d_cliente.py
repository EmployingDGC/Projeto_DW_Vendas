import utilities as utl
import DW_TOOLS as dwt

from sqlalchemy.types import (
    Integer,
    String,
    BigInteger
)


# def get(conn_input):
#     return utl.convert_table_to_dataframe(
#         conn_input=conn_input,
#         schema_name="stage",
#         table_name="STG_CLIENTE",
#         columns=[
#             "id_cliente",
#             "cpf",
#             "nome"
#         ]
#     )


def get(conn_input):
    return dwt.read_table(
        conn=conn_input,
        schema="stage",
        table_name="STG_CLIENTE",
        columns=[
            "id_cliente",
            "cpf",
            "nome",
            "id_endereco",
        ]
    )


def treat(frame):
    order_columns = [
        "SK_CLIENTE",
        "CD_CLIENTE",
        "CD_ENDERECO",
        "CD_CPF",
        "DS_CPF",
        "NO_CLIENTE"
    ]

    return frame.drop_duplicates(
        subset=["cpf", "nome"]
    ).assign(
        NO_CLIENTE=lambda df: utl.convert_column_to_tittle(df.nome),
        CD_CPF=lambda df: utl.convert_column_cpf_to_int64(df.cpf, -3),
        DS_CPF=lambda df: utl.convert_int_cpf_to_format_cpf(df.CD_CPF),
        CD_CLIENTE=lambda df: utl.convert_column_to_int64(df.id_cliente, -3),
        CD_ENDERECO=lambda df: utl.convert_column_to_int64(df.id_endereco, -3),
        SK_CLIENTE=lambda df: utl.create_index_dataframe(df, 1)
    ).pipe(
        func=utl.insert_default_values_table
    ).filter(
        items=order_columns
    )


def run(conn_input):
    dtypes = {
        "SK_CLIENTE": Integer(),
        "CD_CLIENTE": Integer(),
        "CD_ENDERECO": Integer(),
        "CD_CPF": BigInteger(),
        "DS_CPF": String(),
        "NO_CLIENTE": String()
    }

    utl.create_schema(conn_input, "dw")

    get(conn_input).pipe(
        func=treat
    ).to_sql(
        name="D_CLIENTE",
        con=conn_input,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )
