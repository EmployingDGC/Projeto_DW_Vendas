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
#         table_name="STG_FUNCIONARIO",
#         columns=[
#             "id_funcionario",
#             "cpf",
#             "nome"
#         ]
#     )


def extract_dim_funcionario(conn_input):
    return dwt.read_table(
        conn=conn_input,
        schema="stage",
        table_name="STG_FUNCIONARIO",
        columns=[
            "id_funcionario",
            "cpf",
            "nome"
        ]
    )


def treat_dim_funcionario(frame):
    columns_rename = {
        "id_funcionario": "CD_FUNCIONARIO",
        "cpf": "CD_CPF",
        "nome": "NO_FUNCIONARIO"
    }

    order_columns = [
        "SK_FUNCIONARIO",
        "CD_FUNCIONARIO",
        "CD_CPF",
        "DS_CPF",
        "NO_FUNCIONARIO"
    ]

    return frame.drop_duplicates(
        subset=[k for k in frame.keys()]
    ).assign(
        nome=lambda df: utl.convert_column_to_tittle(df.nome),
        cpf=lambda df: utl.convert_column_cpf_to_int64(df.cpf, -3),
        DS_CPF=lambda df: utl.convert_int_cpf_to_format_cpf(df.cpf),
        SK_FUNCIONARIO=lambda df: utl.create_index_dataframe(df, 1)
    ).rename(
        columns=columns_rename
    ).pipe(
        func=utl.insert_default_values_table
    ).filter(
        items=order_columns
    )


def load_dim_funcionario(conn_input):
    dtypes = {
        "SK_FUNCIONARIO": Integer(),
        "CD_FUNCIONARIO": Integer(),
        "CD_CPF": BigInteger(),
        "DS_CPF": String(),
        "NO_FUNCIONARIO": String()
    }

    utl.create_schema(conn_input, "dw")

    extract_dim_funcionario(conn_input).pipe(
        func=treat_dim_funcionario
    ).to_sql(
        name="D_FUNCIONARIO",
        con=conn_input,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )
