from time import time

# import pandas as pd

import connection as conn
import default as dflt
import stage as stg


if __name__ == "__main__":
    time_exec = time()
    time_initial = time()

    conn_db = conn.create_connection_postgre(
        server=dflt.Connection.server,
        database=dflt.Connection.database,
        username=dflt.Connection.username,
        password=dflt.Connection.password,
        port=dflt.Connection.port
    )

    stg.run(conn_db)

    print(f"\nStages carregadas em {round(time() - time_exec)} segundos\n")
    time_exec = time()

    print(f"\nFinalizado com sucesso em {round(time() - time_initial)} segundos\n")
