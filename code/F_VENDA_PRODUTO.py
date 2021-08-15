import utilities as utl
import connection as conn
import DW_TOOLS as dwt

from pandasql import sqldf
import datetime as dt

from sqlalchemy.types import (
    Integer,
    String,
    DateTime,
    Float
)


def extract_fact_venda_produto(connection):
    """
    Extrai os dados para fazer a fato venda
    :param connection: conexão com o banco de dados das stages
    :return: dataframe com o merge das stages venda e item_venda
    """

    if utl.table_exists(
        connection=connection,
        schema_name="dw",
        table_name="f_venda_produto"
    ):
        query = """
            select *
            from stage.stg_venda as sv
            where date(sv.data_venda) > (
                select max(fvp.dt_load)
                from dw.f_venda_produto as fvp
            )
        """

    else:
        query = """
            select *
            from stage.stg_venda
        """

    stg_venda = sqldf(
        query=query,
        db_uri=connection.url
    )

    if stg_venda.empty:
        return stg_venda

    d_cliente = dwt.read_table(
        conn=connection,
        schema="dw",
        table_name="d_cliente",
        columns=[
            "sk_cliente",
            "cd_cliente"
        ]
    )

    d_data = dwt.read_table(
        conn=connection,
        schema="dw",
        table_name="d_data",
        columns=[
            "sk_data",
            "dt_referencia"
        ]
    )

    d_funcionario = dwt.read_table(
        conn=connection,
        schema="dw",
        table_name="d_funcionario",
        columns=[
            "sk_funcionario",
            "cd_funcionario"
        ]
    )

    d_tipo_pagamento = dwt.read_table(
        conn=connection,
        schema="dw",
        table_name="d_tipo_pagamento",
        columns=[
            "sk_tipo_pagamento",
            "cd_tipo_pagamento"
        ]
    )

    stg_item_venda = dwt.read_table(
        conn=connection,
        schema="stage",
        table_name="stg_item_venda"
    )

    f_venda_produto = stg_venda.assign(
        dt_hora_venda=lambda df: df.data_venda.astype("datetime64[ns]").dt.floor("h")
    ).merge(
        right=stg_item_venda,
        how="inner",
        on="id_venda",
        suffixes=("_01", "_02")
    ).pipe(
        func=dwt.merge_input,
        right=d_cliente,
        left_on="id_cliente",
        right_on="cd_cliente",
        suff=["_03", "_04"],
        surrogate_key="sk_cliente"
    ).pipe(
        func=dwt.merge_input,
        right=d_funcionario,
        left_on="id_func",
        right_on="cd_funcionario",
        suff=["_03", "_04"],
        surrogate_key="sk_funcionario"
    ).pipe(
        func=dwt.merge_input,
        right=d_tipo_pagamento,
        left_on="id_pagamento",
        right_on="cd_tipo_pagamento",
        suff=["_03", "_04"],
        surrogate_key="sk_tipo_pagamento"
    ).pipe(
        func=dwt.merge_input,
        right=d_data,
        left_on="dt_hora_venda",
        right_on="dt_referencia",
        suff=["_03", "_04"],
        surrogate_key="sk_data"
    )


def treat_fact_venda_produto(frame, connection):
    """
    Trata os dados para fazer a fato venda
    :param frame: dataframe com os dados extraidos
    :param connection: conexão com o banco de dados das dimensões
    :return: dataframe com os dados tratados para fazer a fato venda
    """




def load_fact_venda_produto(frame, connection):
    """
    Carrega a fato venda no banco de dados do dw
    :param connection: conexão com o banco de dados de saida
    :return: None
    """

    dtypes = {
        "sk_produto": Integer(),
        "sk_cliente": Integer(),
        "sk_loja": Integer(),
        "sk_funcionario": Integer(),
        "sk_data": Integer(),
        "sk_tipo_pagamento": Integer(),
        "sk_endereco_loja": Integer(),
        "sk_endereco_cliente": Integer(),
        "cd_nfc": Integer(),
        "vl_liquido": Float(),
        "vl_bruto": Float(),
        "vl_percentual_lucro": Float(),
        "qtd_produto": Integer()
    }

    frame.to_sql(
        name="f_venda_produto",
        con=connection,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )


def run_fact_venda_produto(connection):
    extract = extract_fact_venda_produto(connection)

    if extract.empty:
        return

    treat = extract.pipe(
        func=treat_fact_venda_produto,
        connection=connection
    )

    treat.pipe(
        func=load_fact_venda_produto,
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

    run_fact_venda_produto(conn_db)
