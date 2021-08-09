import utilities as utl

import connection as conn

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
        "sk_data",
        "dt_referencia",
        "dt_ano",
        "dt_mes",
        "dt_dia",
        "dt_hora",
        "ds_turno"
    ]

    return frame.assign(
        ds_turno=lambda df: df.apply(
            lambda row: (
                "Madrugada" if 0 <= row.dt_hora < 6 else
                "Manhã" if 6 <= row.dt_hora < 12 else
                "Tarde" if 12 <= row.dt_hora < 18 else
                "Noite" if 18 <= row.dt_hora < 24 else
                "Desconhecido"
            ),
            axis=1
        )
    ).pipe(
        func=utl.insert_default_values_table
    ).filter(
        items=order_columns
    )


def load_dim_data(frame, connection):
    """
    Carrega a dimensão data
    :param connection: conexão com o banco de dados de saída
    :return: None
    """

    dtypes = {
        "sk_data": Integer(),
        "dt_referencia": DateTime(),
        "dt_ano": Integer(),
        "dt_mes": Integer(),
        "dt_dia": Integer(),
        "dt_hora": Integer(),
        "ds_turno": String()
    }

    frame.to_sql(
        name="d_data",
        con=connection,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )


def run_dim_data(connection):
    extract_dim_data().pipe(
        func=treat_dim_data
    ).pipe(
        func=load_dim_data,
        connection=connection
    )


if __name__ == "__main__":
    conn_db = conn.create_connection_postgre(
        server="10.0.0.105",
        database="projeto_dw_vendas",
        username="postgres",
        password="itix.123",
        port=5432
    )

    run_dim_data(conn_db)
