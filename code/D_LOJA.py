import datetime as dt

from sqlalchemy.types import (
    Integer,
    String,
    DateTime
)

from pandasql import sqldf

import utilities as utl
import connection as conn
import DW_TOOLS as dwt


def update_dim_loja(d_update, connection):
    query = """
        update dw.d_loja
        set fl_ativo = 0,
            dt_vigencia_fim = now()::date
        where cd_loja in (
            select du.cd_loja
            from d_update as du
        )
        and fl_ativo = 1
    """

    sqldf(
        query=query,
        env={
            "d_update": d_update
        },
        db_uri=connection.url
    )

    # session = sqla.orm.sessionmaker(
    #     bind=connection
    # )()
    #
    # metadata = sqla.MetaData(
    #     bind=connection
    # )
    #
    # datatable = sqla.Table('d_loja', metadata, schema='dw', autoload=True)
    #
    # update = (
    #     sqla.sql.update(datatable).values({
    #         "fl_ativo": 0,
    #         "dt_vigencia_fim": dt.date.today()
    #     }).where(
    #         sqla.and_(
    #             datatable.columns.cd_loja.in_(dim_update.cd_loja),
    #             datatable.columns.fl_ativo == 1
    #         )
    #     )
    # )
    #
    # session.execute(update)
    # session.flush()
    # session.commit()


def extract_dim_loja(connection):
    """
    Extrai os dados para fazer a dimensão loja
    :param connection: conexão com o banco de dados das stages
    :return: dataframe com o merge das stages loja e endereco
    """

    # stg_loja = dwt.read_table(conn=connection, schema="stage", table_name="stg_loja",
    #                           columns=["id_loja", "nome_loja", "razao_social", "cnpj",
    #                                    "id_endereco"])

    stg_loja = dwt.read_table(
        conn=connection,
        schema="stage",
        table_name="stg_loja"
    )

    # stg_endereco = dwt.read_table(conn=connection, schema="stage", table_name="stg_endereco",
    #                               columns=["id_endereco", "estado", "cidade", "bairro",
    #                                        "rua"])

    stg_endereco = dwt.read_table(
        conn=connection,
        schema="stage",
        table_name="stg_endereco"
    )

    # merge_stgs_loja_endereco = (
    #     stg_loja.merge(right=stg_endereco, left_on="id_endereco", right_on="id_endereco",
    #                    how="left", suffixes=["_01", "_02"])
    # )

    merge_stgs_loja_endereco = stg_loja.merge(
        right=stg_endereco,
        how="left",
        on="id_endereco"
    )

    try:
        query = """
            SELECT msld.*, dim.dt_vigencia_fim,
                CASE
                    WHEN dim.cd_loja IS NULL 
                        THEN 'I'
                    WHEN (
                        dim.cd_endereco         != msld.id_endereco
                        OR dim.ds_razao_social  != msld.razao_social
                        OR dim.nu_cnpj          != msld.cnpj
                    )
                        THEN 'U'
                    ELSE 'N'
                END as fl_iun
            FROM merge_stgs_loja_endereco as msld
            LEFT JOIN dw.d_loja as dim 
                ON (msld.id_loja = dim.cd_loja)
                AND dim.fl_ativo = 1
        """

        iun_d_loja = sqldf(
            query=query,
            env={
                "merge_stgs_loja_endereco": merge_stgs_loja_endereco
            },
            db_uri=connection.url
        )

        return iun_d_loja

    except:
        return merge_stgs_loja_endereco


def treat_dim_loja(frame, connection):
    """
    Trata os dados extraidos para fazer a dimensão loja
    :param frame: dataframe com os dados extraidos
    :return: dataframe com os dados tratados para fazer a dimensão loja
    """

    select_columns = [
        "cd_loja",
        "no_loja",
        "ds_razao_social",
        "nu_cnpj",
        "cd_endereco",
        "no_estado",
        "no_cidade",
        "no_bairro",
        "no_rua",
        "fl_ativo",
        "dt_vigencia_inicio",
        "dt_vigencia_fim",
        "fl_iun"
    ]

    rename_columns = {
        "id_loja": "cd_loja",
        "nome_loja": "no_loja",
        "razao_social": "ds_razao_social",
        "cnpj": "nu_cnpj",
        "id_endereco": "cd_endereco",
        "estado": "no_estado",
        "cidade": "no_cidade",
        "bairro": "no_bairro",
        "rua": "no_rua"
    }

    d_loja = frame.rename(
        columns=rename_columns
    ).filter(
        items=select_columns
    ).assign(
        dt_vigencia_inicio=dt.date.today()
    ).assign(
        dt_vigencia_inicio=lambda df: df.dt_vigencia_inicio.astype("datetime64[ns]"),
        fl_ativo=1
    )

    try:
        query = """
            select dl.sk_loja
            from dw.d_loja as dl
        """

        sk_index = max(sqldf(
            query=query,
            db_uri=connection.url
        ).sk_loja) + 1


    except:
        sk_index = 1

        select_columns = [
            "sk_loja",
            "cd_loja",
            "no_loja",
            "ds_razao_social",
            "nu_cnpj",
            "cd_endereco",
            "no_estado",
            "no_cidade",
            "no_bairro",
            "no_rua",
            "dt_vigencia_inicio",
            "dt_vigencia_fim",
            "fl_ativo"
        ]

        d_loja = d_loja.assign(
            dt_vigencia_fim=None,
        ).assign(
            dt_vigencia_fim=lambda df: df.dt_vigencia_fim.astype("datetime64[ns]"),
            sk_loja=lambda df: utl.create_index_dataframe(df, sk_index)
        ).filter(
            items=select_columns
        ).pipe(
            func=utl.insert_default_values_table
        )

        return d_loja

    return d_loja.assign(
        sk_loja=lambda df: utl.create_index_dataframe(df, sk_index)
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

    if utl.table_exists(connection, "dw", "d_loja"):
        d_update = frame.query("fl_iun == 'U'")

        frame = frame.drop(
            columns=["fl_iun"]
        )

        if not d_update.empty:
            update_dim_loja(d_update, connection)

    frame.to_sql(
        name="d_loja",
        con=connection,
        schema="dw",
        if_exists="append",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )


def run_dim_loja(connection):
    extract = extract_dim_loja(
        connection=connection
    ).pipe(
        func=lambda df: df.query(
            expr="fl_iun == 'I' or fl_iun == 'U'"
        )
    )

    if extract.empty:
        return

    treat = extract.pipe(
        func=treat_dim_loja,
        connection=connection
    )

    treat.pipe(
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
