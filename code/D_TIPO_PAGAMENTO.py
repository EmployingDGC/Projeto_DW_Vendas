import utilities as utl
import connection as conn
import DW_TOOLS as dwt

from sqlalchemy.types import (
    Integer,
    String
)


def extract_dim_tipo_pagamento(connection):
    """
    Extrai os dados para fazer a dimensão tipo de pagamento
    :param connection: conexão com o banco de dados das stages
    :return: dataframe com os dados extraidos da stage forma_pagamento
    """

    return dwt.read_table(
        conn=connection,
        schema="stage",
        table_name="stg_forma_pagamento",
        columns=[
            "id_pagamento",
            "nome",
            "descricao"
        ]
    )


def treat_dim_tipo_pagamento(frame, connection):
    """
    Trata os dados para fazer a dimensão tipo_pagamento
    :param frame: dataframe com os dados extraidos
    :return: dataframe com os dados tratados para fazer a dimensão tipo pagamento
    """

    select_columns = [
        "sk_tipo_pagamento",
        "cd_tipo_pagamento",
        "no_tipo_pagamento",
        "ds_tipo_pagamento"
    ]

    rename_columns_x = {
        "sk_tipo_pagamento_x": "sk_tipo_pagamento",
        "cd_tipo_pagamento_x": "cd_tipo_pagamento",
        "no_tipo_pagamento_x": "no_tipo_pagamento",
        "ds_tipo_pagamento_x": "ds_tipo_pagamento"
    }

    new_d_tipo_pagamento = frame.assign(
        cd_tipo_pagamento=lambda df: df.id_pagamento.astype("Int64"),
        no_tipo_pagamento=lambda df: df.nome,
        ds_tipo_pagamento=lambda df: df.descricao,
        sk_tipo_pagamento=lambda df: utl.create_index_dataframe(df, 1)
    ).pipe(
        func=utl.insert_default_values_table
    ).filter(
        items=select_columns
    )

    try:
        old_d_tipo_pagamento = dwt.read_table(
            conn=connection,
            schema="dw",
            table_name="d_tipo_pagamento"
        )

    except:
        return new_d_tipo_pagamento

    return new_d_tipo_pagamento.merge(
        right=old_d_tipo_pagamento,
        how="inner",
        on="cd_tipo_pagamento"
    ).assign(
        fl_trash=lambda df: df.apply(
            lambda row: (
                str(row.cd_tipo_pagamento_x) == str(row.cd_tipo_pagamento_y) and
                str(row.no_tipo_pagamento_x) == str(row.no_tipo_pagamento_y) and
                str(row.ds_tipo_pagamento_x) == str(row.ds_tipo_pagamento_y)
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


def load_dim_tipo_pagamento(frame, connection):
    """
    Carrega os dados da dimensão tipo_pagamento no banco de daods
    :param connection: conexão com o banco de dados de saída
    :return: None
    """

    dtypes = {
        "sk_tipo_pagamento": Integer(),
        "cd_tipo_pagamento": Integer(),
        "no_tipo_pagamento": String(),
        "ds_tipo_pagamento": String()
    }

    frame.to_sql(
        name="d_tipo_pagamento",
        con=connection,
        schema="dw",
        if_exists="append",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )


def run_dim_tipo_pagamento(connection):
    extract_dim_tipo_pagamento(connection).pipe(
        func=treat_dim_tipo_pagamento,
        connection = connection
    ).pipe(
        func=load_dim_tipo_pagamento,
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

    run_dim_tipo_pagamento(conn_db)

