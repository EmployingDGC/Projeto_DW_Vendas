import utilities as utl
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
        table_name="STG_VENDA"
    ).merge(
        right=dwt.read_table(
            conn=connection,
            schema="stage",
            table_name="STG_ITEM_VENDA"
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

    columns_rename = {
        "qtd_produto": "QTD_PRODUTO"
    }

    select_columns = [
        "SK_PRODUTO",
        "SK_CLIENTE",
        "SK_LOJA",
        "SK_FUNCIONARIO",
        "SK_DATA",
        "SK_CATEGORIA",
        "SK_TIPO_PAGAMENTO",
        "SK_ENDERECO_LOJA",
        "SK_ENDERECO_CLIENTE",
        "CD_NFC",
        "VL_LIQUIDO",
        "VL_BRUTO",
        "VL_PERCENTUAL_LUCRO",
        "QTD_PRODUTO"
    ]

    d_produto = dwt.read_table(
        conn=connection,
        schema="dw",
        table_name="D_PRODUTO",
        columns=["SK_PRODUTO", "CD_PRODUTO", "DT_CADASTRO"]
    )

    return frame.assign(
        data_venda=lambda df: df.data_venda.astype("datetime64[ns]"),
        SK_PRODUTO=lambda df: (
            utl.convert_column_to_int64(
                column_data_frame=df.apply(
                    lambda row: (
                        d_produto.query(
                            f"CD_PRODUTO == {row.id_produto}"
                        ).assign(
                            FL_TRASH=lambda df1: df1.DT_CADASTRO.apply(
                                lambda value: row.data_venda < value
                            )
                        ).pipe(
                            lambda df1: df1[~df1.FL_TRASH]
                        ).sort_values(
                            by=["DT_CADASTRO"],
                            ascending=False
                        ).iloc[0].SK_PRODUTO
                    ),
                    axis=1
                ),
                default=-3
            )
        ),
        SK_CLIENTE=lambda df: (
            utl.convert_column_to_int64(
                column_data_frame=df.merge(
                    right=dwt.read_table(
                        conn=connection,
                        schema="dw",
                        table_name="D_CLIENTE",
                        columns=["SK_CLIENTE", "CD_CLIENTE"]
                    ),
                    how="left",
                    left_on="id_cliente",
                    right_on="CD_CLIENTE"
                ).SK_CLIENTE,
                default=-3
            )
        ),
        SK_LOJA=lambda df: (
            utl.convert_column_to_int64(
                column_data_frame=df.merge(
                    right=dwt.read_table(
                        conn=connection,
                        schema="dw",
                        table_name="D_LOJA",
                        columns=["SK_LOJA", "CD_LOJA", "FL_ATIVO"]
                    ).pipe(
                        lambda df1: df1[df1["FL_ATIVO"] == 1]
                    ),
                    how="left",
                    left_on="id_loja",
                    right_on="CD_LOJA"
                ).SK_LOJA,
                default=-3
            )
        ),
        SK_FUNCIONARIO=lambda df: (
            utl.convert_column_to_int64(
                column_data_frame=df.merge(
                    right=dwt.read_table(
                        conn=connection,
                        schema="dw",
                        table_name="D_FUNCIONARIO",
                        columns=["SK_FUNCIONARIO", "CD_FUNCIONARIO"]
                    ),
                    how="left",
                    left_on="id_func",
                    right_on="CD_FUNCIONARIO"
                ).SK_FUNCIONARIO,
                default=-3
            )
        ),
        SK_DATA=lambda df: (
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
                        table_name="D_DATA",
                        columns=["SK_DATA", "DT_REFERENCIA"]
                    ),
                    how="left",
                    left_on="data_venda",
                    right_on="DT_REFERENCIA"
                ).SK_DATA,
                default=-3
            )
        ),
        SK_CATEGORIA=lambda df: (
            utl.create_sk_categoria(
                column=df.merge(
                    right=dwt.read_table(
                        conn=connection,
                        schema="dw",
                        table_name="D_PRODUTO",
                        columns=["SK_PRODUTO", "NO_PRODUTO"]
                    ),
                    how="left",
                    on="SK_PRODUTO"
                ).NO_PRODUTO
            )
        ),
        SK_TIPO_PAGAMENTO=lambda df: (
            utl.convert_column_to_int64(
                column_data_frame=df.merge(
                    right=dwt.read_table(
                        conn=connection,
                        schema="dw",
                        table_name="D_TIPO_PAGAMENTO",
                        columns=["SK_TIPO_PAGAMENTO", "CD_TIPO_PAGAMENTO"]
                    ),
                    how="left",
                    left_on="id_pagamento",
                    right_on="CD_TIPO_PAGAMENTO"
                ).SK_TIPO_PAGAMENTO,
                default=-3
            )
        ),
        CD_NFC=lambda df: (
            utl.convert_column_to_int64(
                column_data_frame=df.nfc,
                default=-3
            )
        ),
        VL_LIQUIDO=lambda df: (
            utl.convert_column_to_float64(
                column_data_frame=df.merge(
                    right=dwt.read_table(
                        conn=connection,
                        schema="stage",
                        table_name="STG_PRODUTO",
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
        VL_PERCENTUAL_LUCRO=lambda df: (
            utl.convert_column_to_float64(
                column_data_frame=df.merge(
                    right=dwt.read_table(
                        conn=connection,
                        schema="stage",
                        table_name="STG_PRODUTO",
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
        VL_BRUTO=lambda df: (
            df.VL_LIQUIDO + df.VL_LIQUIDO * df.VL_PERCENTUAL_LUCRO
        )
    ).rename(
        columns=columns_rename
    ).pipe(
        func=utl.insert_default_values_table
    ).filter(
        items=select_columns
    )


def load_fact_venda_produto(connection):
    """
    Carrega a fato venda no banco de dados do dw
    :param connection: conexão com o banco de dados de saida
    :return: None
    """

    dtypes = {
        "SK_PRODUTO": Integer(),
        "SK_CLIENTE": Integer(),
        "SK_LOJA": Integer(),
        "SK_FUNCIONARIO": Integer(),
        "SK_DATA": Integer(),
        "SK_CATEGORIA": Integer(),
        "SK_TIPO_PAGAMENTO": Integer(),
        "SK_ENDERECO_LOJA": Integer(),
        "SK_ENDERECO_CLIENTE": Integer(),
        "CD_NFC": Integer(),
        "VL_LIQUIDO": Float(),
        "VL_BRUTO": Float(),
        "VL_PERCENTUAL_LUCRO": Float(),
        "QTD_PRODUTO": Integer()
    }

    utl.create_schema(connection, "dw")

    extract_fact_venda_produto(connection).pipe(
        func=treat_fact_venda_produto,
        connection=connection
    ).to_sql(
        name="F_VENDA_PRODUTO",
        con=connection,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )