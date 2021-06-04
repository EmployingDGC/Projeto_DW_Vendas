from sqlalchemy.engine.mock import MockConnection

import pandas as pd
import utilities as utl
import default as dflt


def get(conn_input: MockConnection) -> pd.DataFrame:
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name=dflt.Schema.client,
        table_name=dflt.TablesClient.forma_pagamento
    )


def create(conn_input: MockConnection) -> None:
    get(conn_input).to_sql(
        name=dflt.TablesSTG.forma_pagamento,
        con=conn_input,
        schema=dflt.Schema.stage,
        if_exists="replace",
        index=False,
        chunksize=dflt.Configuration.rows_per_data_frame
    )
