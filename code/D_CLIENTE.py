import utilities as utl
import DW_TOOLS as dwt
import pandas as pd

from sqlalchemy.types import (
    Integer,
    String,
    BigInteger
)


def extract_dim_cliente(connection):
    """
    extrai os dados necessários para criar a dimensão cliente
    :param connection: conexão com o banco de dados das stages
    :return: dataframe com o merge das stages cliente e endereço
    """

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


def treat_dim_cliente(frame, connection):
    """
    Trata os dados extraidos para criar a dimensão cliente
    :param frame: dataframe com os dados extraidos
    :return: dataframe com a dimensão cliente
    """

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

    rename_columns_x = {
        "SK_CLIENTE_x": "SK_CLIENTE",
        "CD_CLIENTE_x": "CD_CLIENTE",
        "NO_CLIENTE_x": "NO_CLIENTE",
        "NU_CPF_x": "NU_CPF",
        "CD_ENDERECO_x": "CD_ENDERECO",
        "NO_ESTADO_x": "NO_ESTADO",
        "NO_CIDADE_x": "NO_CIDADE",
        "NO_BAIRRO_x": "NO_BAIRRO",
        "NO_RUA_x": "NO_RUA"
    }

    new_d_cliente = frame.assign(
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

    try:
        old_d_cliente = dwt.read_table(
            conn=connection,
            schema="dw",
            table_name="D_CLIENTE"
        )

    except:
        return new_d_cliente

    return new_d_cliente.merge(
        right=old_d_cliente,
        how="inner",
        on="CD_CLIENTE"
    ).assign(
        FL_TRASH=lambda df: df.apply(
            lambda row: (
                str(row.NO_CLIENTE_x) == str(row.NO_CLIENTE_y) and
                str(row.NU_CPF_x) == str(row.NU_CPF_y) and
                str(row.CD_ENDERECO_x) == str(row.CD_ENDERECO_y)
            ),
            axis=1
        )
    ).pipe(
        lambda df: df[~df["FL_TRASH"]]
    )


def load_dim_cliente(connection):
    """
    Carrega a dimensão cliente
    :param connection: conexão com o banco de dados de saída
    :return: None
    """

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
        if_exists="append",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )
