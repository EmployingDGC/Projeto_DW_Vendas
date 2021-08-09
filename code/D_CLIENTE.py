import utilities as utl
import DW_TOOLS as dwt
import connection as conn

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
        table_name="stg_cliente",
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
            table_name="stg_endereco"
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

    rename_columns_x = {
        "sk_cliente_x": "sk_cliente",
        "cd_cliente_x": "cd_cliente",
        "no_cliente_x": "no_cliente",
        "nu_cpf_x": "nu_cpf",
        "cd_endereco_x": "cd_endereco",
        "no_estado_x": "no_estado",
        "no_cidade_x": "no_cidade",
        "no_bairro_x": "no_bairro",
        "no_rua_x": "no_rua"
    }

    new_d_cliente = frame.assign(
        no_cliente=lambda df: df.nome,
        nu_cpf=lambda df: df.cpf,
        cd_cliente=lambda df: df.id_cliente,
        cd_endereco=lambda df: df.id_endereco,
        sk_cliente=lambda df: utl.create_index_dataframe(df, 1),
        no_estado=lambda df: df.estado,
        no_cidade=lambda df: df.cidade,
        no_bairro=lambda df: df.bairro,
        no_rua=lambda df: df.rua
    ).pipe(
        func=utl.insert_default_values_table
    ).filter(
        items=select_columns
    )

    try:
        old_d_cliente = dwt.read_table(
            conn=connection,
            schema="dw",
            table_name="d_cliente"
        )

    except:
        return new_d_cliente

    return new_d_cliente.merge(
        right=old_d_cliente,
        how="inner",
        on="cd_cliente"
    ).assign(
        fl_trash=lambda df: df.apply(
            lambda row: (
                str(row.no_cliente_x) == str(row.no_cliente_y) and
                str(row.nu_cpf_x) == str(row.nu_cpf_y) and
                str(row.cd_endereco_x) == str(row.cd_endereco_y)
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


def load_dim_cliente(df, connection):
    """
    Carrega a dimensão cliente
    :param connection: conexão com o banco de dados de saída
    :return: None
    """

    dtypes = {
        "sk_cliente": Integer(),
        "cd_cliente": Integer(),
        "cd_endereco": Integer(),
        "nu_cpf": BigInteger(),
        "no_cliente": String(),
        "no_estado": String(),
        "no_cidade": String(),
        "no_bairro": String(),
        "no_rua": String()
    }

    df.to_sql(
        name="d_cliente",
        con=connection,
        schema="dw",
        if_exists="append",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )


def run_dim_cliente(connection):
    extract_dim_cliente(connection).pipe(
        func=treat_dim_cliente,
        connection=connection
    ).pipe(
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
