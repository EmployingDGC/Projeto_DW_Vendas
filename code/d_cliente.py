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


def extract_dim_cliente(connection):
    return dwt.read_table(
        conn=connection,
        schema="stage",
        table_name="STG_CLIENTE",
        columns=[
            "id_cliente",
            "cpf",
            "nome",
            "id_endereco",
        ]
    ).merge(
        right=dwt.read_table(
            conn=connection,
            schema="stage",
            table_name="STG_ENDERECO"
        ),
        how="inner",
        on="id_endereco"
    )


def treat_dim_cliente(frame):
    select_columns = [
        "SK_CLIENTE",
        "CD_CLIENTE",
        "NO_CLIENTE",
        "NU_CPF",
        "CD_ENDERECO",
        "NO_ESTADO",
        "NO_CIDADE",
        "NO_BAIRRO",
        "NO_RUA"
    ]

    return frame.assign(
        NO_CLIENTE=lambda df: df.nome.str.upper(),
        NU_CPF=lambda df: df.cpf.str.replace("-", ""),
        CD_CLIENTE=lambda df: utl.convert_column_to_int64(df.id_cliente, -3),
        CD_ENDERECO=lambda df: utl.convert_column_to_int64(df.id_endereco, -3),
        SK_CLIENTE=lambda df: utl.create_index_dataframe(df, 1),
        NO_ESTADO=lambda df: df.estado.str.upper().str.strip(),
        NO_CIDADE=lambda df: df.cidade.str.upper().str.strip(),
        NO_BAIRRO=lambda df: df.bairro.str.upper().str.strip(),
        NO_RUA=lambda df: df.rua.str.upper().str.strip()
    ).pipe(
        func=utl.insert_default_values_table
    ).filter(
        items=select_columns
    )


def load_dim_cliente(connection):
    dtypes = {
        "SK_CLIENTE": Integer(),
        "CD_CLIENTE": Integer(),
        "CD_ENDERECO": Integer(),
        "CD_CPF": BigInteger(),
        "DS_CPF": String(),
        "NO_CLIENTE": String(),
        "NO_ESTADO": String(),
        "NO_CIDADE": String(),
        "NO_BAIRRO": String(),
        "NO_RUA": String()
    }

    utl.create_schema(connection, "dw")

    extract_dim_cliente(connection).pipe(
        func=treat_dim_cliente
    ).to_sql(
        name="D_CLIENTE",
        con=connection,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )
