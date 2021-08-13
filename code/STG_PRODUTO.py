import utilities as utl
import connection as conn
import DW_TOOLS as dwt

import pandas as pd

import datetime as dt


def extract_stg_produto(connection):
    """
    Extrai os dados para fazer a slow change stage produto
    :param connection: conexão com o banco de dados do cliente
    :return: dataframe com os dados extraidos para fazer a stage produto
    """

    df_produto = dwt.read_table(
        conn=connection,
        schema="public",
        table_name="PRODUTO"
    )

    try:
        stg_produto = dwt.read_table(
            conn=connection,
            schema="stage",
            table_name="stg_produto"
        )

    except:
        return df_produto

    return pd.concat([
        stg_produto,
        df_produto
    ]).drop_duplicates()


def treat_stg_produto(frame):
    """
    Trata os dados para fazer a slow change stage produto
    :param frame: dataframe com os dados extraidos
    :param conn_output: conexão com o banco de daos das stages
    :return: dataframe com os dados tratados
    """

    try:
        frame.data_final

    except:
        return frame.assign(
            data_final=None
        )

    return frame


def load_stg_produto(frame, connection):
    """
    Carrega a slow change stage produto
    :param connection: conexão com o banco de dados do cliente
    :return: None
    """

    frame.to_sql(
        name="stg_produto",
        con=connection,
        schema="stage",
        if_exists="replace",
        index=False,
        chunksize=10000
    )


def run_stg_produto(connection):
    extract_stg_produto(connection).pipe(
        func=treat_stg_produto
    ).pipe(
        func=load_stg_produto,
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

    run_stg_produto(conn_db)
