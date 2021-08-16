import utilities as utl
import connection as conn
import DW_TOOLS as dwt

from pandasql import sqldf

from sqlalchemy.types import (
    Integer,
    String
)


def update_dim_tipo_pagamento(df_update, connection):
    query = """
        delete from
            dw.d_tipo_pagamento
        where
            cd_tipo_pagamento in (
                select
                    du.id_pagamento
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


def extract_dim_tipo_pagamento(connection):
    """
    Extrai os dados para fazer a dimensão tipo de pagamento
    :param connection: conexão com o banco de dados das stages
    :return: dataframe com os dados extraidos da stage forma_pagamento
    """

    d_tipo_pagamento_exists = utl.table_exists(
        connection=connection,
        schema_name="dw",
        table_name="d_tipo_pagamento"
    )

    if not d_tipo_pagamento_exists:
        query = """
            select
                sfp.*,
                'I' as fl_iun
            from
                stage.stg_forma_pagamento as sfp
        """

    else:
        query = """
            select
                sfp.*,
                dtp.*,
                case
                    when dtp.cd_tipo_pagamento is null
                        then 'I'
                    when (
                        dtp.no_tipo_pagamento    != sfp.nome
                        or dtp.ds_tipo_pagamento != sfp.descricao
                    )
                        then 'U'
                    else 'N'
                end as fl_iun
            from
                stage.stg_forma_pagamento as sfp 
            left join
                dw.d_tipo_pagamento as dtp
            on
                sfp.id_pagamento = dtp.cd_tipo_pagamento
        """

    stg_forma_pagamento = sqldf(
        query=query,
        db_uri=connection.url
    )

    return stg_forma_pagamento


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

    d_tipo_pagamento_exists = utl.table_exists(
        connection=connection,
        schema_name="dw",
        table_name="d_tipo_pagamento"
    )

    if d_tipo_pagamento_exists:
        query = """
            select
                dtp.sk_tipo_pagamento
            from
                dw.d_tipo_pagamento as dtp
        """

        sk_index = max(
            sqldf(
                query=query,
                db_uri=connection.url
            ).sk_tipo_pagamento
        ) + 1

        update_d_tipo_pagamento = frame.query(
            expr="fl_iun == 'U'"
        )

        update_dim_tipo_pagamento(
            df_update=update_d_tipo_pagamento,
            connection=connection
        )

    else:
        sk_index = 1

    d_tipo_pagamento = frame.assign(
        cd_tipo_pagamento=lambda df: df.id_pagamento.astype("Int64"),
        no_tipo_pagamento=lambda df: df.nome,
        ds_tipo_pagamento=lambda df: df.descricao,
        sk_tipo_pagamento=lambda df: utl.create_index_dataframe(df, sk_index)
    ).filter(
        items=select_columns
    )

    if not d_tipo_pagamento_exists:
        d_tipo_pagamento = d_tipo_pagamento.pipe(
            func=utl.insert_default_values_table
        )

    return d_tipo_pagamento


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
    extract = extract_dim_tipo_pagamento(
        connection=connection
    ).pipe(
        func=lambda df: df.query(
            expr="fl_iun == 'I' or fl_iun == 'U'"
        )
    )

    if extract.empty:
        return

    treat = extract.pipe(
        func=treat_dim_tipo_pagamento,
        connection=connection
    )

    treat.pipe(
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

