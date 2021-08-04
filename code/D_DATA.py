import utilities as utl

from sqlalchemy.types import (
    Integer,
    String,
    DateTime
)


def extract_dim_data():
    """
    extrai os dados necessários para criar a dimensão data
    :return: None
    """

    return utl.generate_date_table(
        start_date="2016-01-01",
        end_date="2030-01-01"
    )


def treat_dim_data(frame):
    """
    Trata os dados extraidos para criar a dimensão data
    :param frame: dataframe com os dados extraidos
    :return: dataframe com a dimensão data pronta
    """

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


def load_dim_data(connection):
    """
    Carrega a dimensão data
    :param connection: conexão com o banco de dados de saída
    :return: None
    """

    dtypes = {
        "SK_DATA": Integer(),
        "DT_REFERENCIA": DateTime(),
        "DT_ANO": Integer(),
        "DT_MES": Integer(),
        "DT_DIA": Integer(),
        "DT_HORA": Integer(),
        "DS_TURNO": String()
    }

    utl.create_schema(connection, "dw")

    extract_dim_data().pipe(
        func=treat_dim_data
    ).to_sql(
        name="D_DATA",
        con=connection,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )