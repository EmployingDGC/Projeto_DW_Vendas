import utilities as utl
import connection as conn
import DW_TOOLS as dwt

from sqlalchemy.types import (
    Integer,
    String,
    DateTime
)


def extract_dim_produto(connection):
    """
    Extrai os dados para fazer a dimensão produto
    :param connection: conexão com o banco de dados das stages
    :return: dataframe com os dados extraidos da stage produto
    """

    stg_produto = dwt.read_table(
        conn=connection,
        schema="stage",
        table_name="stg_produto",
        columns=[
            "id_produto",
            "cod_barra",
            "nome_produto",
            "data_cadastro",
            "ativo"
        ]
    )

    return stg_produto


def treat_dim_produto(frame):
    """
    Trata os dados extraidos para fazer a dimensão produto
    :param frame: dataframe com os dados extraidos
    :return: dataframe com os dados tratados para fazer a dimensão produto
    """

    select_columns = [
        "sk_produto",
        "cd_produto",
        "cd_barras",
        "no_produto",
        "dt_cadastro",
        "fl_ativo"
    ]

    return frame.assign(
        cd_produto=lambda df: df.id_produto.astype("Int64"),
        cd_barra=lambda df: df.cod_barra.astype("Int64"),
        dt_cadastro=lambda df: df.data_cadastro.astype("datetime64[ns]"),
        no_produto=lambda df: df.nome_produto,
        fl_ativo=lambda df: df.ativo.astype("int64"),
        sk_produto=lambda df: utl.create_index_dataframe(df, 1)
    ).filter(
        items=select_columns
    ).pipe(
        func=utl.insert_default_values_table
    )


def load_dim_produto(frame, connection):
    """
    Carrega a dimensão produto no banco de dados
    :param connection: conexão com o banco de dados de saída
    :return: None
    """

    dtypes = {
        "sk_produto": Integer(),
        "cd_produto": Integer(),
        "cd_barras": Integer(),
        "no_produto": String(),
        "dt_cadastro": DateTime(),
        "fl_ativo": Integer()
    }

    frame.to_sql(
        name="d_produto",
        con=connection,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )


def run_dim_produto(connection):
    extract_dim_produto(connection).pipe(
        func=treat_dim_produto
    ).pipe(
        func=load_dim_produto,
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

    run_dim_produto(conn_db)
