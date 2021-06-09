import pandas as pd

import utilities as utl


def get():
    all_turnos = [
        "Manhã",
        "Tarde",
        "Noite",
        "Madrugada"
    ]

    return pd.DataFrame(
        data={
            "SK_TURNO": [i + 1 for i in range(len(all_turnos))],
            "DS_TURNO": [c for c in all_turnos]
        }
    ).pipe(
        func=utl.insert_default_values_table
    )


# def treat() -> pd.DataFrame:
#     pass


def run(conn_input):
    utl.create_schema(conn_input, "dw")

    get().to_sql(
        name="D_TURNO",
        con=conn_input,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000
    )
