from time import time

import pandas as pd

import connection as conn


def run_stg(connection):
    pass


def run_dms(connection):
    pass


def run_fact(connection):
    pass


def run(connection):
    run_stg(connection)
    run_dms(connection)
    run_fact(connection)


if __name__ == "__main__":
    time_exec = time()
    time_initial = time()

    pd.set_option("display.max_columns", None)

    db_connection = conn.create_connection_postgre(
        server="10.0.0.105",
        database="projeto_dw_vendas_refatorado",
        username="postgres",
        password="itix.123",
        port=5432
    )

    # run(db_connection)

    print(f"\nFinalizado com sucesso em {round(time() - time_initial)} segundos\n")
