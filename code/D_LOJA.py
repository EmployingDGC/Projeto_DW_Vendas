import utilities as utl
import connection as conn
import DW_TOOLS as dwt

from sqlalchemy.types import (
    Integer,
    String,
    DateTime
)


def extract_dim_loja(connection):
    """
    Extrai os dados para fazer a dimensão loja
    :param connection: conexão com o banco de dados das stages
    :return: dataframe com o merge das stages loja e endereco
    """

    return dwt.read_table(
        conn=connection,
        schema="stage",
        table_name="stg_loja"
    ).merge(
        right=dwt.read_table(
            conn=connection,
            schema="stage",
            table_name="stg_endereco"
        ),
        how="inner",
        on="id_endereco"
    )


def treat_dim_loja(frame):
    """
    Trata os dados extraidos para fazer a dimensão loja
    :param frame: dataframe com os dados extraidos
    :return: dataframe com os dados tratados para fazer a dimensão loja
    """

    columns_rename = {
        "id_loja": "cd_loja",
        "id_endereco": "cd_endereco",
        "telefone": "nu_telefone",
        "data_inicial": "dt_inicial",
        "data_final": "dt_final",
        "ativo": "fl_ativo"
    }

    select_columns = [
        "sk_loja",
        "cd_loja",
        "cd_endereco",
        "nu_cnpj",
        "nu_telefone",
        "no_loja",
        "no_razao_social",
        "no_estado",
        "no_cidade",
        "no_bairro",
        "no_rua",
        "dt_inicial",
        "dt_final",
        "fl_ativo"
    ]

    return frame.assign(
        nu_cnpj=lambda df: df.cnpj,
        no_loja=lambda df: df.nome_loja,
        no_razao_social=lambda df: df.razao_social,
        sk_loja=lambda df: utl.create_index_dataframe(df, 1),
        no_estado=lambda df: df.estado,
        no_cidade=lambda df: df.cidade,
        no_bairro=lambda df: df.bairro,
        no_rua=lambda df: df.rua,
        dt_inicial=lambda df: df.data_inicial.astype("datetime64[ns]"),
        dt_final=lambda df: df.data_final.astype("datetime64[ns]")
    ).rename(
        columns=columns_rename
    ).pipe(
        func=utl.insert_default_values_table
    ).filter(
        items=select_columns
    )


def load_dim_loja(frame, connection):
    """
    Carrega os dados tratados para fazer a dimensão loja
    :param connection: conexão com o banco de dados de saída
    :return: None
    """

    dtypes = {
        "sk_loja": Integer(),
        "cd_loja": Integer(),
        "cd_endereco": Integer(),
        "nu_cnpj": String(),
        "nu_telefone": String(),
        "no_loja": String(),
        "no_razao_social": String(),
        "no_estado": String(),
        "no_cidade": String(),
        "no_bairro": String(),
        "no_rua": String(),
        "dt_inicial": DateTime(),
        "dt_final": DateTime(),
        "fl_ativo": Integer()
    }

    frame.to_sql(
        name="D_LOJA",
        con=connection,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )


def run_dim_loja(connection):
    extract_dim_loja(connection).pipe(
        func=treat_dim_loja
    ).pipe(
        func=load_dim_loja,
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

    run_dim_loja(conn_db)
