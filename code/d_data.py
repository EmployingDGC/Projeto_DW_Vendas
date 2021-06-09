import pandas as pd

import utilities as utl


def get(conn_input):
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name="stage",
        table_name="STG_VENDA",
        columns=[
            "data_venda"
        ]
    )


def treat(frame):
    columns_rename = {
        "data_venda": "DT_REFERENCIA"
    }

    order_columns = [
        "SK_DATA",
        "DT_REFERENCIA",
        "DT_HORA",
        "DT_DIA",
        "DT_MES",
        "DT_ANO"
    ]

    return frame.drop_duplicates(
        subset=["data_venda"]
    ).assign(
        data_venda=lambda df: pd.to_datetime(df.data_venda),
        DT_HORA=lambda df: utl.convert_column_datetime_to_hour(df.data_venda, -3),
        DT_DIA=lambda df: utl.convert_column_datetime_to_day(df.data_venda, -3),
        DT_MES=lambda df: utl.convert_column_datetime_to_month(df.data_venda, -3),
        DT_ANO=lambda df: utl.convert_column_datetime_to_year(df.data_venda, -3),
        SK_DATA=lambda df: utl.create_index_dataframe(df, 1)
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
        name="D_DATA",
        con=conn_input,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000
    )
