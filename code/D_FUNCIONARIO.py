import utilities as utl
import connection as conn
import DW_TOOLS as dwt

import pandas as pd

from pandasql import sqldf

from sqlalchemy.types import (
    Integer,
    String,
    DateTime
)


def update_dim_funcionario(df_update, connection):
    query = """
        delete from
            dw.d_funcionario
        where
            cd_funcionario in (
                select
                    du.id_funcionario
                from
                    df_update as du
            )
    """

    sqldf(
        query=query,
        env={
            "df_update": df_update
        },
        db_uri=connection.url
    )


def extract_dim_funcionario(connection):
    """
    Extrai os dados para fazer a dimensão funcionario
    :param connection: conexão com o banco de dados das stages
    :return: dataframe com os dados extraidos
    """

    d_funcionario_exists = utl.table_exists(
        connection=connection,
        schema_name="dw",
        table_name="d_funcionario"
    )

    if not d_funcionario_exists:
        query = """
            select
                sf.*,
                'I' as fl_iun
            from
                stage.stg_funcionario as sf
        """

    else:
        query = """
            select
                sf.*,
                df.*,
                case
                    when df.cd_funcionario is null
                        then 'I'
                    when (
                        df.no_funcionario != sf.nome
                        or df.nu_cpf      != sf.cpf
                        or df.nu_telefone != sf.tel
                    )
                        then 'U'
                    else 'N'
                end as fl_iun
            from
                stage.stg_funcionario as sf
            left join
                dw.d_funcionario as df
            on
                sf.id_funcionario = df.cd_funcionario
        """

    stg_funcionario = sqldf(
        query=query,
        db_uri=connection.url
    )

    return stg_funcionario


def treat_dim_funcionario(frame, connection):
    """
    Trata os dados para fazer a dimensão funcionario
    :param frame: dataframe com os dados extraídos
    :return: dataframe com a dimensão funcionario
    """

    select_columns = [
        "sk_funcionario",
        "cd_funcionario",
        "no_funcionario",
        "nu_cpf",
        "nu_telefone",
        "dt_nascimento"
    ]

    d_funcionario_exists = utl.table_exists(
        connection=connection,
        schema_name="dw",
        table_name="d_funcionario"
    )

    if d_funcionario_exists:
        query = """
            select
                df.sk_funcionario
            from
                dw.d_funcionario as df
        """

        sk_index = max(
            sqldf(
                query=query,
                db_uri=connection.url
            ).sk_produto
        ) + 1

        update_d_funcionario = frame.query(
            expr="fl_iun == 'U'"
        )

        update_dim_funcionario(
            df_update=update_d_funcionario,
            connection=connection
        )

    else:
        sk_index = 1

    d_funcionario = frame.assign(
        cd_funcionario=lambda df: df.id_funcionario.astype("Int64"),
        no_funcionario=lambda df: df.nome,
        nu_cpf=lambda df: df.cpf,
        nu_telefone=lambda df: df.tel,
        dt_nascimento=lambda df: pd.to_datetime(df.data_nascimento, format="%d/%m/%y"),
        sk_funcionario=lambda df: utl.create_index_dataframe(df, sk_index)
    ).filter(
        items=select_columns
    )

    if not d_funcionario_exists:
        d_funcionario = d_funcionario.pipe(
            func=utl.insert_default_values_table
        )

    return d_funcionario


def load_dim_funcionario(frame, connection):
    """
    Carrega os dados da dimensão funcionario no dw
    :param connection: conexão com o banco de dados de saída
    :return: None
    """

    dtypes = {
        "sk_funcionario": Integer(),
        "cd_funcionario": Integer(),
        "no_funcionario": String(),
        "nu_cpf": String(),
        "nu_telefone": String(),
        "dt_nascimento": DateTime(),
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
    extract = extract_dim_funcionario(
        connection=connection
    ).pipe(
        func=lambda df: df.query(
            "fl_iun == 'I' or fl_iun == 'U'"
        )
    )

    if extract.empty:
        return

    treat = extract.pipe(
        func=treat_dim_funcionario,
        connection=connection
    )

    treat.pipe(
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
