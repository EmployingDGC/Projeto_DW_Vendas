import pandas as pd

import utilities as utl

from sqlalchemy.types import (
    Integer,
    String
)


def extract_dim_categoria():
    """
    extrai os dados necessários para criar a dimensão categoria
    :return: dataframe com as categorias
    """

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


def load_dim_categoria(connection):
    """
    Carrega a dimensão categoria
    :param connection: conexão com o banco de dados de saída
    :return: None
    """
    dtypes = {
        "SK_CATEGORIA": Integer(),
        "DS_CATEGORIA": String()
    }

    utl.create_schema(connection, "dw")

    extract_dim_categoria().to_sql(
        name="D_CATEGORIA",
        con=connection,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )
