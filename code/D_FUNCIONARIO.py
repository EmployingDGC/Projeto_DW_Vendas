import utilities as utl
import connection as conn
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
        table_name="stg_funcionario",
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
        "id_funcionario": "cd_funcionario",
        "cpf": "nu_cpf",
        "nome": "no_funcionario"
    }

    select_columns = [
        "sk_funcionario",
        "cd_funcionario",
        "nu_cpf",
        "no_funcionario"
    ]

    rename_columns_x = {
        "sk_funcionario_x": "sk_funcionario",
        "cd_funcionario_x": "cd_funcionario",
        "nu_cpf_x": "nu_cpf",
        "no_funcionario_x": "no_funcionario"
    }

    new_d_funcionario = frame.drop_duplicates(
        subset=[k for k in frame.keys()]
    ).assign(
        sk_funcionario=lambda df: utl.create_index_dataframe(df, 1)
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
            table_name="d_cliente"
        )

    except:
        return new_d_funcionario

    return new_d_funcionario.merge(
        right=old_d_funcionario,
        how="inner",
        on="cd_funcionario"
    ).assign(
        fl_trash=lambda df: df.apply(
            lambda row: (
                str(row.nu_cpf_x) == str(row.nu_cpf_y) and
                str(row.no_funcionario_x) == str(row.no_funcionario_y)
            ),
            axis=1
        )
    ).pipe(
        lambda df: df[~df["fl_trash"]]
    ).rename(
        columns=rename_columns_x
    ).filter(
        items=select_columns
    )


def load_dim_funcionario(frame, connection):
    """
    Carrega os dados da dimensão funcionario no dw
    :param connection: conexão com o banco de dados de saída
    :return: None
    """

    dtypes = {
        "sk_funcionario": Integer(),
        "cd_funcionario": Integer(),
        "nu_cpf": BigInteger(),
        "no_funcionario": String()
    }

    utl.create_schema(connection, "dw")

    frame.to_sql(
        name="d_funcionario",
        con=connection,
        schema="dw",
        if_exists="append",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )


def run_dim_funcionario(connection):
    extract_dim_funcionario(connection).pipe(
        func=treat_dim_funcionario
    ).pipe(
        func=load_dim_funcionario,
        connection=connection
    )


if __name__ == "__main__":
    conn_db = conn.create_connection_postgre(
        server="10.0.0.105",
        database="projeto_dw_vendas",
        username="postgres",
        password="itix.123",
        port=5432
    )

    run_dim_funcionario(conn_db)
