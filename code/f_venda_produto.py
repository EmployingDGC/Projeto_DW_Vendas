import utilities as utl
import DW_TOOLS as dwt


# def get(conn_input):
#     return utl.convert_table_to_dataframe(
#         conn_input=conn_input,
#         schema_name="stage",
#         table_name="STG_VENDA",
#         columns=[
#             "id_pagamento",
#             "id_cliente",
#             "id_func",
#             "id_loja",
#             "nfc"
#         ]
#     )


def get(conn_input):
    return dwt.read_table(
        conn=conn_input,
        schema="stage",
        table_name="STG_VENDA"
    ).merge(
        right=dwt.read_table(
            conn=conn_input,
            schema="stage",
            table_name="STG_ITEM_VENDA"
        ),
        how="inner",
        on="id_venda"
    )


def treat(frame, connection):
    columns_rename = {
        "qtd_produto": "QTD_PRODUTO"
    }

    order_columns = [
        "SK_PRODUTO",
        "SK_CLIENTE",
        "SK_LOJA",
        "SK_FUNCIONARIO",
        "SK_DATA",
        "SK_CATEGORIA",
        "SK_TIPO_PAGAMENTO",
        "SK_ENDERECO",
        "CD_NFC",
        "VL_LIQUIDO",
        "VL_BRUTO",
        "VL_PERCENTUAL_LUCRO",
        "QTD_PRODUTO"
    ]

    return frame.assign(
        SK_PRODUTO=lambda df: utl.convert_column_to_int64(
            column_data_frame=df.merge(
                right=dwt.read_table(
                    conn=connection,
                    schema="dw",
                    table_name="D_PRODUTO",
                    columns=["SK_PRODUTO", "CD_PRODUTO", "FL_ATIVO"]
                ).pipe(
                    lambda df1: df1[df1["FL_ATIVO"] == 1]
                ),
                how="left",
                left_on="id_produto",
                right_on="CD_PRODUTO"
            ).SK_PRODUTO,
            default=-3
        ),
        SK_CLIENTE=lambda df: utl.convert_column_to_int64(
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
        ),
        SK_LOJA=lambda df: utl.convert_column_to_int64(
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
        ),
        SK_FUNCIONARIO=lambda df: utl.convert_column_to_int64(
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
        ),
        SK_DATA=lambda df: utl.convert_column_to_int64(
            column_data_frame=df.assign(
                data_venda=lambda df1: df1.data_venda.apply(
                    lambda value: f"{str(value).split(':', 1)[0]}:00:00"
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
        ),
        SK_CATEGORIA=lambda df: utl.create_sk_categoria(
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
        ),
        SK_TIPO_PAGAMENTO=lambda df: utl.convert_column_to_int64(
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
        ),
        SK_ENDERECO=lambda df: utl.convert_column_to_int64(
            column_data_frame=df.merge(
                right=dwt.read_table(
                    conn=connection,
                    schema="dw",
                    table_name="D_LOJA",
                    columns=["SK_LOJA", "CD_ENDERECO"]
                ),
                how="left",
                on="SK_LOJA"
            ).merge(
                right=dwt.read_table(
                    conn=connection,
                    schema="dw",
                    table_name="D_ENDERECO",
                    columns=["SK_ENDERECO", "CD_ENDERECO"]
                ),
                how="left",
                on="CD_ENDERECO"
            ).SK_ENDERECO,
            default=-3
        ),
        CD_NFC=lambda df: utl.convert_column_to_int64(
            column_data_frame=df.nfc,
            default=-3
        ),
        VL_LIQUIDO=lambda df: utl.convert_column_to_float64(
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
        ),
        VL_PERCENTUAL_LUCRO=lambda df: utl.convert_column_to_float64(
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
        ),
        VL_BRUTO=lambda df: df.VL_LIQUIDO + df.VL_LIQUIDO * df.VL_PERCENTUAL_LUCRO
    ).rename(
        columns=columns_rename
    ).pipe(
        func=utl.insert_default_values_table
    ).filter(
        items=order_columns
    )


def run(conn_input):
    utl.create_schema(conn_input, "dw")

    get(conn_input).pipe(
        func=treat,
        connection=conn_input
    ).to_sql(
        name="F_VENDA_PRODUTO",
        con=conn_input,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000
    )
