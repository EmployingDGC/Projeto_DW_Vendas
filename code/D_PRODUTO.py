import utilities as utl
import connection as conn
import DW_TOOLS as dwt

import unidecode as ud

import datetime as dt

from pandasql import sqldf

import pandas as pd

from sqlalchemy.types import (
    Integer,
    String,
    DateTime
)


def get_category_dim_produto(product_name):
    product_ud = ud.unidecode(product_name.upper())
    split_product_ud = product_ud.split(" ")

    all_categories = {
        "Café da Manhã": [
            "CAFE",
            "ACHOCOLATADO",
            "CEREAL",
            "CEREAIS",
            "PAO",
            "ACUCAR",
            "ADOCANTE",
            "BISCOITO",
            "GELEIA",
            "IORGUTE"
        ],
        "Mercearia": [
            "ARROZ",
            "FEIJAO",
            "FARINHA",
            "TRIGO"
            "AMIDO",
            "FERMENTO",
            "MACARRAO",
            "MOLHO",
            "AZEITE",
            "OLEO",
            "OVOS",
            "TEMPEROS",
            "SAL",
            "SAZON",
            "FARINHA",
            "AVEIA",
            "FANDANGOS"
        ],
        "Carnes": [
            "BIFE",
            "FRANGO",
            "PEIXE",
            "CARNE MOIDA",
            "SALSICHA",
            "LINGUICA"
        ],
        "Bebidas": [
            "SUCO",
            "CERVEJA",
            "REFRIGERANTE",
            "VINHO"
        ],
        "Higiene": [
            "SABONETE",
            "CREME DENTAL",
            "SHAMPOO",
            "CONDICIONADOR",
            "ABSORVENTE",
            "PAPEL HIGIENICO",
            "FRALDA"
        ],
        "Laticínios / Frios": [
            "LEITE",
            "PRESUNTO",
            "QUEIJO",
            "REQUEIJAO",
            "MANTEIGA",
            "CREME DE LEITE"
        ],
        "Limpeza": [
            "AGUA SANITARIA",
            "SABAO",
            "PALHA DE ACO",
            "AMACIANTE",
            "DETERGENTE",
            "SACO",
            "DESINFETANTE",
            "PAPEL TOALHA"
        ],
        "Hortifruti": [
            "ALFACE",
            "CEBOLA",
            "ALHO",
            "TOMATE",
            "LIMAO",
            "BANANA",
            "MACA",
            "BATATA"
        ]
    }

    for k, v in all_categories.items():
        join_v = " ".join(v)

        if product_ud in join_v:
            return k

        if len(split_product_ud) > 1:
            for w in split_product_ud:
                if w in join_v:
                    return k

    return None


def update_dim_produto(df_update, connection):
    query = """
        update
            dw.d_produto
        set
            fl_ativo = 0,
            dt_vigencia_fim = now()::timestamp
        where
            cd_produto in (
                select
                    du.id_produto
                from
                    df_update as du
            )
        and
            fl_ativo = 1
    """

    sqldf(
        query=query,
        env={
            "df_update": df_update
        },
        db_uri=connection.url
    )


def extract_dim_produto(connection):
    """
    Extrai os dados para fazer a dimensão produto
    :param connection: conexão com o banco de dados das stages
    :return: dataframe com os dados extraidos da stage produto
    """

    d_produto_exists = utl.table_exists(
        connection=connection,
        schema_name="dw",
        table_name="d_produto"
    )

    if d_produto_exists:
        query = """
            select
                sp.*,
                dp.dt_vigencia_inicio,
                dp.dt_vigencia_fim,
                case
                    when dp.cd_produto is null
                        then 'I'
                    when (
                        dp.vl_custo               != sp.preco_custo
                        or dp.vl_percentual_lucro != sp.percentual_lucro
                        or dp.no_produto          != sp.nome_produto
                        or dp.cd_barra            != sp.cod_barra
                    )
                        then 'U'
                    else 'N'
                end as fl_iun
            from
                stage.stg_produto as sp
            left join
                dw.d_produto as dp
            on
                sp.id_produto = dp.cd_produto
            where
                dp.fl_ativo = 1
                or dp.fl_ativo is null
        """

    else:
        query = """
            select sv.*, 'I' as fl_iun
            from stage.stg_produto as sv
        """

    stg_produto = sqldf(
        query=query,
        db_uri=connection.url
    )

    return stg_produto


def treat_dim_produto(frame, connection):
    """
    Trata os dados extraidos para fazer a dimensão produto
    :param frame: dataframe com os dados extraidos
    :return: dataframe com os dados tratados para fazer a dimensão produto
    """

    d_produto_exists = utl.table_exists(
        connection=connection,
        schema_name="dw",
        table_name="d_produto"
    )

    select_columns = [
        "sk_produto",
        "cd_produto",
        "no_produto",
        "ds_categoria",
        "cd_barra",
        "vl_custo",
        "vl_percentual_lucro",
        "dt_vigencia_inicio",
        "dt_vigencia_fim",
        "fl_ativo"
    ]

    if d_produto_exists:
        query = """
            select
                dp.sk_produto
            from
                dw.d_produto as dp
        """

        sk_index = max(
            sqldf(
                query=query,
                db_uri=connection.url
            ).sk_produto
        ) + 1

        df_update = frame.query(
            expr="fl_iun == 'U'"
        )

        update_dim_produto(
            df_update=df_update,
            connection=connection
        )

    else:
        sk_index = 1

    d_produto = frame.assign(
        cd_produto=lambda df: df.id_produto.astype("Int64"),
        no_produto=lambda df: df.nome_produto,
        cd_barra=lambda df: df.cod_barra.astype("Int64"),
        vl_custo=lambda df: df.preco_custo,
        vl_percentual_lucro=lambda df: df.percentual_lucro,
        dt_vigencia_inicio=lambda df: (
            pd.to_datetime(frame.data_cadastro, format="%d/%m/%Y")
            if not d_produto_exists
            else dt.datetime.now()
        ),
        dt_vigencia_fim=None,
        fl_ativo=1,
        sk_produto=lambda df: utl.create_index_dataframe(df, sk_index)
    ).assign(
        dt_vigencia_fim=lambda df: df.dt_vigencia_fim.astype("datetime64[ns]"),
        ds_categoria=lambda df: df.no_produto.apply(
            func=lambda v: get_category_dim_produto(v)
        )
    ).filter(
        items=select_columns
    )

    if not d_produto_exists:
        d_produto = d_produto.pipe(
            func=utl.insert_default_values_table
        ).assign(
            vl_custo=lambda df: pd.concat([
                pd.Series([-3.0, -2.0, -1.0]),
                df.vl_custo[3:]
            ]),
            vl_percentual_lucro=lambda df: pd.concat([
                pd.Series([-3.0, -2.0, -1.0]),
                df.vl_percentual_lucro[3:]
            ])
        )

    return d_produto


def load_dim_produto(frame, connection):
    """
    Carrega a dimensão produto no banco de dados
    :param connection: conexão com o banco de dados de saída
    :return: None
    """

    dtypes = {
        "sk_produto": Integer(),
        "cd_produto": Integer(),
        "no_produto": String(),
        "ds_categoria": String(),
        "cd_barra": Integer(),
        "vl_custo": String(),
        "vl_percentual_lucro": String(),
        "dt_vigencia_inicio": DateTime(),
        "dt_vigencia_fim": DateTime(),
        "fl_ativo": Integer()
    }

    frame.to_sql(
        name="d_produto",
        con=connection,
        schema="dw",
        if_exists="append",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )


def run_dim_produto(connection):
    extract = extract_dim_produto(
        connection=connection
    ).pipe(
        func=lambda df: df.query(
            expr="fl_iun == 'I' or fl_iun == 'U'"
        )
    )

    if extract.empty:
        return

    treat = extract.pipe(
        func=treat_dim_produto,
        connection=connection
    )

    treat.pipe(
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
