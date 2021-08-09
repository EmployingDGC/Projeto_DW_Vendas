import utilities as utl
import connection as conn
import DW_TOOLS as dwt

from sqlalchemy.types import (
    Integer,
    Float
)


def extract_fact_venda_produto(connection):
    """
    Extrai os dados para fazer a fato venda
    :param connection: conexão com o banco de dados das stages
    :return: dataframe com o merge das stages venda e item_venda
    """

    return dwt.read_table(
        conn=connection,
        schema="stage",
        table_name="stg_venda"
    ).merge(
        right=dwt.read_table(
            conn=connection,
            schema="stage",
            table_name="stg_item_venda"
        ),
        how="inner",
        on="id_venda"
    )


def treat_fact_venda_produto(frame, connection):
    """
    Trata os dados para fazer a fato venda
    :param frame: dataframe com os dados extraidos
    :param connection: conexão com o banco de dados das dimensões
    :return: dataframe com os dados tratados para fazer a fato venda
    """

    select_columns = [
        "sk_produto",
        "sk_cliente",
        "sk_loja",
        "sk_funcionario",
        "sk_data",
        "sk_categoria",
        "sk_tipo_pagamento",
        "sk_endereco_loja",
        "sk_endereco_cliente",
        "cd_nfc",
        "vl_liquido",
        "vl_bruto",
        "vl_percentual_lucro",
        "qtd_produto"
    ]

    d_produto = dwt.read_table(
        conn=connection,
        schema="dw",
        table_name="d_produto",
        columns=["sk_produto", "cd_produto", "dt_cadastro"]
    )

    return frame.assign(
        data_venda=lambda df: df.data_venda.astype("datetime64[ns]"),
        sk_produto=lambda df: (
            utl.convert_column_to_int64(
                column_data_frame=df.apply(
                    lambda row: (
                        d_produto.query(
                            f"cd_produto == {row.id_produto}"
                        ).assign(
                            FL_TRASH=lambda df1: df1.dt_cadastro.apply(
                                lambda value: row.data_venda < value
                            )
                        ).pipe(
                            lambda df1: df1[~df1.FL_TRASH]
                        ).sort_values(
                            by=["dt_cadastro"],
                            ascending=False
                        ).iloc[0].sk_produto
                    ),
                    axis=1
                ),
                default=-3
            )
        ),
        sk_cliente=lambda df: (
            utl.convert_column_to_int64(
                column_data_frame=df.merge(
                    right=dwt.read_table(
                        conn=connection,
                        schema="dw",
                        table_name="d_cliente",
                        columns=["sk_cliente", "cd_cliente"]
                    ),
                    how="left",
                    left_on="id_cliente",
                    right_on="cd_cliente"
                ).sk_cliente,
                default=-3
            )
        ),
        sk_loja=lambda df: (
            utl.convert_column_to_int64(
                column_data_frame=df.merge(
                    right=dwt.read_table(
                        conn=connection,
                        schema="dw",
                        table_name="d_loja",
                        columns=["sk_loja", "cd_loja", "fl_ativo"]
                    ).pipe(
                        lambda df1: df1[df1["fl_ativo"] == 1]
                    ),
                    how="left",
                    left_on="id_loja",
                    right_on="cd_loja"
                ).sk_loja,
                default=-3
            )
        ),
        sk_funcionario=lambda df: (
            utl.convert_column_to_int64(
                column_data_frame=df.merge(
                    right=dwt.read_table(
                        conn=connection,
                        schema="dw",
                        table_name="d_funcionario",
                        columns=["sk_funcionario", "cd_funcionario"]
                    ),
                    how="left",
                    left_on="id_func",
                    right_on="cd_funcionario"
                ).sk_funcionario,
                default=-3
            )
        ),
        sk_data=lambda df: (
            utl.convert_column_to_int64(
                column_data_frame=df.assign(
                    data_venda=lambda df1: (
                        df1.data_venda.apply(
                            lambda value: f"{str(value).split(':', 1)[0]}:00:00"
                        ).astype("datetime64[ns]")
                    )
                ).merge(
                    right=dwt.read_table(
                        conn=connection,
                        schema="dw",
                        table_name="d_data",
                        columns=["sk_data", "dt_referencia"]
                    ),
                    how="left",
                    left_on="data_venda",
                    right_on="dt_referencia"
                ).sk_data,
                default=-3
            )
        ),
        sk_tipo_pagamento=lambda df: (
            utl.convert_column_to_int64(
                column_data_frame=df.merge(
                    right=dwt.read_table(
                        conn=connection,
                        schema="dw",
                        table_name="d_tipo_pagamento",
                        columns=["sk_tipo_pagamento", "cd_tipo_pagamento"]
                    ),
                    how="left",
                    left_on="id_pagamento",
                    right_on="cd_tipo_pagamento"
                ).sk_tipo_pagamento,
                default=-3
            )
        ),
        cd_nfc=lambda df: (
            utl.convert_column_to_int64(
                column_data_frame=df.nfc,
                default=-3
            )
        ),
        vl_liquido=lambda df: (
            utl.convert_column_to_float64(
                column_data_frame=df.merge(
                    right=dwt.read_table(
                        conn=connection,
                        schema="stage",
                        table_name="stg_produto",
                        columns=["id_produto", "preco_custo", "ativo"]
                    ).pipe(
                        lambda df1: df1[df1["ativo"] == "1"]
                    ),
                    how="left",
                    on="id_produto"
                ).preco_custo,
                default=-3
            )
        ),
        vl_percentual_lucro=lambda df: (
            utl.convert_column_to_float64(
                column_data_frame=df.merge(
                    right=dwt.read_table(
                        conn=connection,
                        schema="stage",
                        table_name="stg_produto",
                        columns=["id_produto", "percentual_lucro", "ativo"]
                    ).pipe(
                        lambda df1: df1[df1["ativo"] == "1"]
                    ),
                    how="left",
                    on="id_produto"
                ).percentual_lucro,
                default=-3
            )
        ),
        vl_bruto=lambda df: (
            df.vl_liquido + df.vl_liquido * df.vl_percentual_lucro
        )
    ).pipe(
        func=utl.insert_default_values_table
    ).filter(
        items=select_columns
    )


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
    extract_fact_venda_produto(connection).pipe(
        func=treat_fact_venda_produto,
        connection=connection
    ).pipe(
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
