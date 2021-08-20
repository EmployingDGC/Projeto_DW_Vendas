import utilities as utl
import DW_TOOLS as dwt
import connection as conn

from pandasql import sqldf

from sqlalchemy.types import (
    Integer,
    String
)


def update_dim_cliente(df_update, connection):
    query = """
        delete from
            dw.d_cliente
        where
            cd_cliente in (
                select
                    du.id_cliente
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


def extract_dim_cliente(connection):
    d_cliente_exist = utl.table_exists(
        connection=connection,
        schema_name="dw",
        table_name="d_cliente"
    )

    stg_cliente = dwt.read_table(
        conn=connection,
        schema="stage",
        table_name="stg_cliente"
    )

    stg_endereco = dwt.read_table(
        conn=connection,
        schema="stage",
        table_name="stg_endereco"
    )

    stg_merge = stg_cliente.merge(
        right=stg_endereco,
        how="left",
        on="id_endereco"
    )

    if not d_cliente_exist:
        return stg_merge.assign(
            fl_iun="I"
        )

    query = """
        select
            sm.*,
            case
                when dc.cd_endereco is null
                    then 'I'
                when (
                    dc.cd_endereco   != sm.id_endereco
                    or dc.no_cliente != sm.nome
                    or dc.nu_cpf     != sm.cpf
                )
                    then 'U'
                else 'N'
            end as fl_iun
        from
            stg_merge as sm
        left join
            dw.d_cliente as dc
        on
            sm.id_cliente = dc.cd_cliente
    """

    df_merges = sqldf(
        query=query,
        env={
            "stg_merge": stg_merge
        },
        db_uri=connection.url
    )

    return df_merges.pipe(
        func=lambda df: df.query(
            expr="fl_iun == 'I' or fl_iun == 'U'"
        )
    )


def treat_dim_cliente(frame, connection):
    d_cliente_exist = utl.table_exists(
        connection=connection,
        schema_name="dw",
        table_name="d_cliente"
    )

    rename_columns = {
        "id_cliente": "cd_cliente",
        "nome": "no_cliente",
        "cpf": "nu_cpf",
        "tel": "nu_telefone",
        "id_endereco": "cd_endereco",
        "estado": "no_estado",
        "cidade": "no_cidade",
        "bairro": "no_bairro",
        "rua": "no_rua"
    }

    select_columns = [
        "sk_cliente",
        "cd_cliente",
        "no_cliente",
        "nu_cpf",
        "nu_telefone",
        "cd_endereco",
        "no_estado",
        "no_cidade",
        "no_bairro",
        "no_rua"
    ]

    if d_cliente_exist:
        query = """
            select
                dc.sk_cliente
            from
                dw.d_cliente as dc
        """

        sk_index = max(
            sqldf(
                query=query,
                db_uri=connection.url
            ).sk_cliente
        ) + 1

        update_d_cliente = frame.query(
            expr="fl_iun == 'U'"
        )

        if not update_d_cliente.empty:
            update_dim_cliente(
                df_update=update_d_cliente,
                connection=connection
            )

    else:
        sk_index = 1

    d_cliente = frame.assign(
        sk_cliente=lambda df: utl.create_index_dataframe(df, sk_index)
    ).rename(
        columns=rename_columns
    ).filter(
        items=select_columns
    )

    if d_cliente_exist:
        return d_cliente

    return d_cliente.pipe(
        func=utl.insert_default_values_table
    )


def load_dim_cliente(frame, connection):
    dtypes = {
        "sk_cliente": Integer(),
        "cd_cliente": Integer(),
        "no_cliente": String(),
        "nu_cpf": String(),
        "nu_telefone": String(),
        "cd_endereco": Integer(),
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
    extract = extract_dim_cliente(
        connection=connection
    )

    if extract.empty:
        return

    treat = extract.pipe(
        func=treat_dim_cliente,
        connection=connection
    )

    treat.pipe(
        func=load_dim_cliente,
        connection=connection
    )


if __name__ == "__main__":
    connection = conn.create_connection_postgre(
        server="10.0.0.105",
        database="projeto_dw_vendas",
        username="postgres",
        password="itix.123",
        port=5432
    )

    run_dim_cliente(connection)
