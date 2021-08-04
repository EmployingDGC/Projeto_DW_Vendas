import utilities as utl
import DW_TOOLS as dwt

from sqlalchemy.types import (
    Integer,
    String,
    BigInteger
)


def extract_dim_funcionario(connection):
    """
    Extrai os dados para fazer a dimensão funcionario
    :param connection: conexão com o banco de dados das stages
    :return: dataframe com os dados extraidos
    """

    return dwt.read_table(
        conn=connection,
        schema="stage",
        table_name="STG_FUNCIONARIO",
        columns=[
            "id_funcionario",
            "cpf",
            "nome"
        ]
    )


def treat_dim_funcionario(frame, connection):
    """
    Trata os dados para fazer a dimensão funcionario
    :param frame: dataframe com os dados extraídos
    :return: dataframe com a dimensão funcionario
    """

    columns_rename = {
        "id_funcionario": "CD_FUNCIONARIO",
        "cpf": "CD_CPF",
        "nome": "NO_FUNCIONARIO"
    }

    select_columns = [
        "SK_FUNCIONARIO",
        "CD_FUNCIONARIO",
        "CD_CPF",
        "DS_CPF",
        "NO_FUNCIONARIO"
    ]

    rename_columns_x = {
        "SK_FUNCIONARIO_x": "SK_FUNCIONARIO",
        "CD_FUNCIONARIO_x": "CD_FUNCIONARIO",
        "CD_CPF_x": "CD_CPF",
        "DS_CPF_x": "DS_CPF",
        "NO_FUNCIONARIO_x": "NO_FUNCIONARIO"
    }

    new_d_funcionario = frame.drop_duplicates(
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
        items=select_columns
    )

    try:
        old_d_funcionario = dwt.read_table(
            conn=connection,
            schema="dw",
            table_name="D_CLIENTE"
        )

    except:
        return new_d_funcionario

    return new_d_funcionario.merge(
        right=old_d_funcionario,
        how="inner",
        on="CD_FUNCIONARIO"
    ).assign(
        FL_TRASH=lambda df: df.apply(
            lambda row: (
                str(row.DS_CPF_x) == str(row.DS_CPF_y) and
                str(row.NO_FUNCIONARIO_x) == str(row.NO_FUNCIONARIO_y)
            ),
            axis=1
        )
    ).pipe(
        lambda df: df[~df["FL_TRASH"]]
    ).rename(
        columns=rename_columns_x
    ).filter(
        items=select_columns
    )


def load_dim_funcionario(connection):
    """
    Carrega os dados da dimensão funcionario no dw
    :param connection: conexão com o banco de dados de saída
    :return: None
    """

    dtypes = {
        "SK_FUNCIONARIO": Integer(),
        "CD_FUNCIONARIO": Integer(),
        "CD_CPF": BigInteger(),
        "DS_CPF": String(),
        "NO_FUNCIONARIO": String()
    }

    utl.create_schema(connection, "dw")

    extract_dim_funcionario(connection).pipe(
        func=treat_dim_funcionario,
        connection=connection
    ).to_sql(
        name="D_FUNCIONARIO",
        con=connection,
        schema="dw",
        if_exists="append",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )
