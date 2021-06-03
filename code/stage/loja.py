from sqlalchemy.engine.mock import MockConnection

import pandas as pd
import utilities as utl
import default as dflt


def get(conn_input: MockConnection) -> pd.DataFrame:
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name=dflt.Schemas.client,
        table_name=dflt.TablesClient.loja
    )


def create(conn_input: MockConnection) -> None:
    frame = get(conn_input)

    frame.to_sql(
        name=dflt.TablesSTG.loja,
        con=conn_input,
        schema=dflt.Schemas.stage,
        if_exists="replace",
        index=False,
        chunksize=1000
    )
