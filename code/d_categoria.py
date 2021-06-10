import pandas as pd

import utilities as utl


def get():
    all_categories = [
        "Café da Manhã",
        "Mercearia",
        "Carnes",
        "Bebidas",
        "Higiene",
        "Laticínios / Frios",
        "Limpeza",
        "Hortifruti"
    ]

    return pd.DataFrame(
        data={
            "SK_CATEGORIA": [i + 1 for i in range(len(all_categories))],
            "DS_CATEGORIA": [c for c in all_categories]
        }
    ).pipe(
        func=utl.insert_default_values_table
    )


# def treat() -> pd.DataFrame:
#     pass


def run(conn_input):
    utl.create_schema(conn_input, "dw")

    get().to_sql(
        name="D_CATEGORIA",
        con=conn_input,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000
    )
