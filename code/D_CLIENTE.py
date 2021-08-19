import utilities as utl
import DW_TOOLS as dwt
import connection as conn

from sqlalchemy.types import (
    Integer,
    String
)


def extract_dim_cliente(connection):
    """
    extrai os dados necessários para criar a dimensão cliente
    :param connection: conexão com o banco de dados das stages
    :return: dataframe com o merge das stages cliente e endereço
    """

    d_cliente_exists = utl.table_exists(
        connection=connection,
        schema_name="dw",
        table_name="d_cliente"
    )

    stg_cliente = dwt.read_table(
        conn=connection,
        schema="stage",
        table_name="stg_cliente",
        columns=[
            "id_cliente",
            "cpf",
            "nome",
            "id_endereco",
        ]
    )

    stg_endereco = dwt.read_table(
        conn=connection,
        schema="stage",
        table_name="stg_endereco"
    )

    merge_stg_cliente_endereco = stg_cliente.merge(
        right=stg_endereco,
        how="left",
        on="id_endereco"
    )

    if not d_cliente_exists:
        return merge_stg_cliente_endereco

    old_d_cliente = dwt.read_table(
        conn=connection,
        schema="dw",
        table_name="d_cliente"
    )

    merge_stgs_dim = merge_stg_cliente_endereco.merge(
        right=old_d_cliente,
        how="left",
        left_on="id_cliente",
        right_on="cd_cliente"
    ).pipe(
        func=lambda df: df[df.cd_cliente.isnull()]
    )

    return merge_stgs_dim


def treat_dim_cliente(frame, connection):
    """
    Trata os dados extraidos para criar a dimensão cliente
    :param frame: dataframe com os dados extraidos
    :return: dataframe com a dimensão cliente
    """

    d_cliente_exist = utl.table_exists(
        connection=connection,
        schema_name="dw",
        table_name="d_cliente"
    )

    if d_cliente_exist:
        last_sk = dwt.read_table(
            conn=connection,
            schema="dw",
            table_name="d_cliente",
            columns=["sk_cliente"]
        ).sk_cliente.max() + 1

    else:
        last_sk = 1

    select_columns_new = [
        "sk_cliente",
        "id_cliente",
        "nome",
        "cpf",
        "id_endereco",
        "estado",
        "cidade",
        "bairro",
        "rua"
    ]

    select_columns_old = [
        "sk_cliente",
        "cd_cliente",
        "no_cliente",
        "nu_cpf",
        "cd_endereco",
        "no_estado",
        "no_cidade",
        "no_bairro",
        "no_rua"
    ]

    rename_columns = {
        "id_cliente": "cd_cliente",
        "nome": "no_cliente",
        "cpf": "nu_cpf",
        "id_endereco": "cd_endereco",
        "estado": "no_estado",
        "cidade": "no_cidade",
        "bairro": "no_bairro",
        "rua": "no_rua"
    }

    if not "sk_cliente" in frame.columns.to_list():
        d_cliente = frame.filter(
            items=select_columns_new
        ).rename(
            columns=rename_columns,
        ).assign(
            sk_cliente=lambda df: utl.create_index_dataframe(df, last_sk)
        ).filter(
            items=select_columns_old
        ).pipe(
            func=utl.insert_default_values_table
        )

    else:
        d_cliente = frame.filter(
            items=select_columns_old
        )

    return d_cliente


def load_dim_cliente(frame, connection):
    """
    Carrega a dimensão cliente
    :param connection: conexão com o banco de dados de saída
    :return: None
    """

    dtypes = {
        "sk_cliente": Integer(),
        "cd_cliente": Integer(),
        "cd_endereco": Integer(),
        "nu_cpf": String(),
        "no_cliente": String(),
        "no_estado": String(),
        "no_cidade": String(),
        "no_bairro": String(),
        "no_rua": String()
    }

    frame.to_sql(
        name="d_cliente",
        con=connection,
        schema="dw",
        if_exists="append",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )


def run_dim_cliente(connection):
    df_extract = extract_dim_cliente(connection)

    if df_extract.empty:
        return

    df_treat = df_extract.pipe(
        func=treat_dim_cliente,
        connection=connection
    )

    df_treat.pipe(
        func=load_dim_cliente,
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

    run_dim_cliente(conn_db)
