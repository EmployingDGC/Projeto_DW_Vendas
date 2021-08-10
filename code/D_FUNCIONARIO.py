import utilities as utl
import connection as conn
import DW_TOOLS as dwt

from sqlalchemy.types import (
    Integer,
    String
)


def extract_dim_funcionario(connection):
    """
    Extrai os dados para fazer a dimensão funcionario
    :param connection: conexão com o banco de dados das stages
    :return: dataframe com os dados extraidos
    """

    stg_funcionario = dwt.read_table(
        conn=connection,
        schema="stage",
        table_name="stg_funcionario",
        columns=[
            "id_funcionario",
            "cpf",
            "nome"
        ]
    )

    try:
        old_d_funcionario = dwt.read_table(
            conn=connection,
            schema="dw",
            table_name="d_funcionario"
        )

    except:
        return stg_funcionario

    new_d_funcionario = stg_funcionario.merge(
        right=old_d_funcionario,
        how="left",
        left_on="id_funcionario",
        right_on="cd_funcionario"
    )[3:]

    return new_d_funcionario


def treat_dim_funcionario(frame, connection):
    """
    Trata os dados para fazer a dimensão funcionario
    :param frame: dataframe com os dados extraídos
    :return: dataframe com a dimensão funcionario
    """

    try:
        last_sk = frame.iloc[-1].sk_funcionario + 1

    except:
        last_sk = 1

    select_columns = [
        "sk_funcionario",
        "cd_funcionario",
        "nu_cpf",
        "no_funcionario"
    ]

    try:
        d_funcionario = frame.assign(
            fl_trash=lambda df: df.apply(
                lambda row: (
                        str(row.nu_cpf) == str(row.cpf)
                        and str(row.no_funcionario) == str(row.nome)
                ),
                axis=1
            )
        ).pipe(
            func=lambda df: df[~df["fl_trash"]]
        )

    except:
        d_funcionario = frame.pipe(
            func=lambda df: utl.insert_default_values_table(df)
        )

    return d_funcionario.assign(
        cd_funcionario=lambda df: df.id_funcionario.astype("Int64"),
        nu_cpf=lambda df: df.cpf,
        no_funcionario=lambda df: df.nome,
        sk_funcionario=lambda df: utl.create_index_dataframe(df, last_sk)
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
        "nu_cpf": String(),
        "no_funcionario": String()
    }

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
        func=treat_dim_funcionario,
        connection=connection
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
