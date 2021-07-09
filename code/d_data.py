import pandas as pd

import utilities as utl

from sqlalchemy.types import (
    Integer,
    String,
    DateTime
)


def get():
    return utl.generate_date_table(
        initial_date="2016-01-01",
        final_date="2030-01-01"
    )


def treat(frame):
    order_columns = [
        "SK_DATA",
        "DT_REFERENCIA",
        "DT_ANO",
        "DT_MES",
        "DT_DIA",
        "DT_HORA",
        "DS_TURNO"
    ]

    return frame.assign(
        DS_TURNO=lambda df: df.apply(
            lambda row: (
                "Madrugada" if 0 <= row.DT_HORA < 6 else
                "Manhã" if 6 <= row.DT_HORA < 12 else
                "Tarde" if 12 <= row.DT_HORA < 18 else
                "Noite" if 18 <= row.DT_HORA < 24 else
                "Desconhecido"
            ),
            axis=1
        )
    ).pipe(
        func=utl.insert_default_values_table
    ).filter(
        items=order_columns
    )


def run(conn_input):
    dtypes = {
        "SK_DATA": Integer(),
        "DT_REFERENCIA": DateTime(),
        "DT_ANO": Integer(),
        "DT_MES": Integer(),
        "DT_DIA": Integer(),
        "DT_HORA": Integer(),
        "DS_TURNO": String()
    }

    utl.create_schema(conn_input, "dw")

    get().pipe(
        func=treat
    ).to_sql(
        name="D_DATA",
        con=conn_input,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )
