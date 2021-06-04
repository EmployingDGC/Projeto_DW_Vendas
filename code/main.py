from time import time

# import pandas as pd

import connection as conn
import default as dflt
import stage as stg
import dimension as dms


if __name__ == "__main__":
    time_exec = time()
    time_initial = time()

    conn_db = conn.create_connection_postgre(
        server=dflt.Configuration.Connection.server,
        database=dflt.Configuration.Connection.database,
        username=dflt.Configuration.Connection.username,
        password=dflt.Configuration.Connection.password,
        port=dflt.Configuration.Connection.port
    )

    stg.run(conn_db)

    print(f"\nStages carregadas em {round(time() - time_exec)} segundos\n")
    time_exec = time()

    dms.run(conn_db)

    print(f"\nDimensions carregadas em {round(time() - time_exec)} segundos\n")
    time_exec = time()

    print(f"\nFinalizado com sucesso em {round(time() - time_initial)} segundos\n")
