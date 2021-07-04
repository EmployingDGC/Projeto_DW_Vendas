import utilities as utl
import DW_TOOLS as dwt


# def get(conn_input):
#     return utl.convert_table_to_dataframe(
#         conn_input=conn_input,
#         schema_name="stage",
#         table_name="STG_FORMA_PAGAMENTO",
#         columns=[
#             "id_pagamento",
#             "nome",
#             "descricao"
#         ]
#     )


def get(conn_input):
    return dwt.read_table(
        conn=conn_input,
        schema="stage",
        table_name="STG_FORMA_PAGAMENTO",
        columns=[
            "id_pagamento",
            "nome",
            "descricao"
        ]
    )


def treat(frame):
    columns_rename = {
        "id_pagamento": "CD_PAGAMENTO",
        "nome": "NO_PAGAMENTO",
        "descricao": "DS_PAGAMENTO"
    }

    order_columns = [
        "SK_PAGAMENTO",
        "CD_PAGAMENTO",
        "NO_PAGAMENTO",
        "DS_PAGAMENTO"
    ]

    return frame.assign(
        nome=lambda df: utl.convert_column_to_upper(df.nome),
        descricao=lambda df: utl.convert_column_to_upper(df.descricao),
        SK_PAGAMENTO=lambda df: utl.create_index_dataframe(df, 1)
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
        func=treat
    ).to_sql(
        name="D_TIPO_PAGAMENTO",
        con=conn_input,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000
    )
