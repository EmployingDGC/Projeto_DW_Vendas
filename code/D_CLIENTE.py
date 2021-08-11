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
        how="inner",
        on="id_endereco"
    )

    try:
        old_d_cliente = dwt.read_table(
            conn=connection,
            schema="dw",
            table_name="d_cliente"
        )

    except:
        return merge_stg_cliente_endereco

    merge_stgs_dim = merge_stg_cliente_endereco.merge(
        right=old_d_cliente,
        how="left",
        left_on="id_cliente",
        right_on="cd_cliente"
    )

    return merge_stgs_dim


def treat_dim_cliente(frame, connection):
    """
    Trata os dados extraidos para criar a dimensão cliente
    :param frame: dataframe com os dados extraidos
    :return: dataframe com a dimensão cliente
    """

    try:
        last_sk = frame.iloc[-1].sk_cliente + 1

    except:
        last_sk = 1

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

    try:
        d_cliente_temp = frame.assign(
            fl_trash=lambda df: df.apply(
                lambda row: (
                        row.nome == row.no_cliente and
                        row.cpf == row.nu_cpf and
                        row.id_endereco == row.cd_endereco
                ),
                axis=1
            )
        ).pipe(
            lambda df: df[~df["fl_trash"]]
        )

    except:
        d_cliente_temp = frame.pipe(
            func=utl.insert_default_values_table
        )

    d_cliente = d_cliente_temp.assign(
        cd_cliente=lambda df: df.id_cliente.astype("Int64"),
        nu_cpf=lambda df: df.cpf,
        no_cliente=lambda df: df.nome,
        cd_endereco=lambda df: df.id_endereco.astype("Int64"),
        no_estado=lambda df: df.estado,
        no_cidade=lambda df: df.cidade,
        no_bairro=lambda df: df.bairro,
        no_rua=lambda df: df.rua,
    )

    d_cliente.apply(
        lambda row: utl.delete_register_from_table(
            conn_output=connection,
            schema_name="dw",
            table_name="d_cliente",
            where=f"sk_cliente = {row.sk_cliente}"
        ),
        axis=1
    )

    return d_cliente.assign(
        sk_cliente=lambda df: utl.create_index_dataframe(df, last_sk)
    ).filter(
        items=select_columns
    )


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
