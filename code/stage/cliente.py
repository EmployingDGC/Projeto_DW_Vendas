from sqlalchemy.engine.mock import MockConnection

import pandas as pd
import utilities as utl
import default as DFLT


def get(conn_input: MockConnection) -> pd.DataFrame:
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name=DFLT.Schemas.client,
        table_name=DFLT.TablesClient.cliente
    )


def create(conn_input: MockConnection) -> None:
    frame = get(conn_input)

    frame.to_sql(
        name=DFLT.TablesSTG.cliente,
        con=conn_input,
        schema=DFLT.Schemas.stage,
        if_exists="replace",
        index=False,
        chunksize=1000
    )
