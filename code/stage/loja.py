from sqlalchemy.engine.mock import MockConnection

import pandas as pd
import utilities as utl
import default as dflt


def get(conn_input: MockConnection) -> pd.DataFrame:
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name=dflt.schema.client,
        table_name=dflt.tables.client.loja
    )


def create(conn_input: MockConnection) -> None:
    get(conn_input).to_sql(
        name=dflt.tables.stage.loja,
        con=conn_input,
        schema=dflt.schema.stage,
        if_exists="replace",
        index=False,
        chunksize=dflt.configuration.rows_per_data_frame
    )
