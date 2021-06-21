import pandas as pd

import utilities as utl

import numpy as np

import stg_venda
import stg_item_venda
import stg_produto

import d_categoria
import d_cliente
import d_data
import d_endereco
import d_funcionario
import d_loja
import d_produto
import d_tipo_pagamento
import d_turno


def get(conn_input):
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name="stage",
        table_name="STG_VENDA",
        columns=[
            "id_pagamento",
            "id_cliente",
            "id_func",
            "id_loja",
            "nfc"
        ]
    )


def treat(frame, connection):
    columns_rename = {
        "id_pagamento": "SK_TIPO_PAGAMENTO",
        "id_cliente": "SK_CLIENTE",
        "id_func": "SK_FUNCIONARIO",
        "id_loja": "SK_LOJA",
        "nfc": "CD_NFC"
    }

    order_columns = [
        "SK_PRODUTO",
        "SK_CLIENTE",
        "SK_LOJA",
        "SK_FUNCIONARIO",
        "SK_DATA",
        "SK_CATEGORIA",
        "SK_TURNO",
        "SK_TIPO_PAGAMENTO",
        "SK_ENDERECO",
        "CD_NFC",
        "VL_LIQUIDO",
        "VL_BRUTO",
        "VL_PERCENTUAL_LUCRO",
        "QTD_PRODUTO"
    ]

    return frame.assign(
        id_pagamento=lambda df: utl.convert_column_to_int64(
            column_data_frame=df.merge(
                right=d_tipo_pagamento.get(connection),
                how="inner",
                on="id_pagamento"
            ).id_pagamento,
            default=-3
        ),
        id_cliente=lambda df: df.merge(
            right=d_cliente.get(connection),
            how="inner",
            on="id_cliente"
        ).id_cliente,
        id_func=lambda df: utl.convert_column_to_int64(
            column_data_frame=df.merge(
                right=d_funcionario.get(connection),
                how="inner",
                right_on="id_funcionario",
                left_on="id_func"
            ).id_func,
            default=-3
        ),
        id_loja=lambda df: utl.convert_column_to_int64(
            column_data_frame=df.merge(
                right=d_loja.get(connection),
                how="inner",
                on="id_loja"
            ).id_loja,
            default=-3
        ),
        nfc=lambda df: utl.convert_column_to_int64(df.nfc, -3),
        SK_DATA=lambda df: stg_venda.get(connection).id_venda,
        SK_PRODUTO=lambda df: utl.convert_column_to_int64(
            column_data_frame=stg_venda.get(connection).merge(
                right=stg_item_venda.get(connection),
                how="inner",
                on="id_venda"
            ).merge(
                right=d_produto.get(connection).pipe(
                    func=d_produto.treat
                ),
                how="inner",
                right_on="CD_PRODUTO",
                left_on="id_produto"
            ).SK_PRODUTO,
            default=-3
        ),
        SK_ENDERECO=lambda df: utl.convert_column_to_int64(
            column_data_frame=df.merge(
                right=d_loja.get(connection),
                how="inner",
                on="id_loja"
            ).id_endereco,
            default=-3
        ),
        SK_CATEGORIA=lambda df: utl.create_sk_categoria(
            column=df.merge(
                right=utl.convert_table_to_dataframe(
                    conn_input=connection,
                    schema_name="dw",
                    table_name="D_PRODUTO",
                    columns=[
                        "CD_PRODUTO",
                        "NO_PRODUTO"
                    ]
                ),
                how="inner",
                right_on="CD_PRODUTO",
                left_on="SK_PRODUTO"
            ).NO_PRODUTO
        ),
        SK_TURNO=lambda df: df.merge(
            right=utl.convert_table_to_dataframe(
                conn_input=connection,
                schema_name="dw",
                table_name="D_DATA",
                columns=[
                    "SK_DATA",
                    "DT_HORA"
                ]
            ),
            how="inner",
            on="SK_DATA",
        ).DT_HORA,
        QTD_PRODUTO=lambda df: utl.convert_column_to_int64(
            column_data_frame=stg_venda.get(connection).merge(
                right=stg_item_venda.get(connection),
                how="inner",
                on="id_venda"
            ).merge(
                right=d_produto.get(connection).pipe(
                    func=d_produto.treat
                ),
                how="inner",
                right_on="CD_PRODUTO",
                left_on="id_produto"
            ).qtd_produto,
            default=-3
        ),
        VL_LIQUIDO=lambda df: utl.convert_column_to_float64(
            column_data_frame=df.merge(
                right=stg_produto.get(connection),
                how="inner",
                right_on="id_produto",
                left_on="SK_PRODUTO"
            ).preco_custo,
            default=-3
        ),
        VL_PERCENTUAL_LUCRO=lambda df: utl.convert_column_to_float64(
            column_data_frame=df.merge(
                right=stg_produto.get(connection),
                how="inner",
                right_on="id_produto",
                left_on="SK_PRODUTO"
            ).percentual_lucro,
            default=-3
        ),
        VL_BRUTO=lambda df: df.VL_LIQUIDO * df.VL_PERCENTUAL_LUCRO + df.VL_LIQUIDO
    ).assign(
        id_cliente=lambda df: utl.convert_column_to_int64(df.id_cliente, -3),
        SK_TURNO=lambda df: utl.create_sk_turno(df.SK_TURNO)
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
