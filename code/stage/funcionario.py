from sqlalchemy.engine.mock import MockConnection

import pandas as pd
import utilities as utl
import default as dflt


def get(conn_input: MockConnection) -> pd.DataFrame:
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name=dflt.schema.client,
        table_name=dflt.tables.client.funcionario
    )


def create(conn_input: MockConnection) -> None:
    get(conn_input).to_sql(
        name=dflt.tables.stage.funcionario,
        con=conn_input,
        schema=dflt.schema.stage,
        if_exists=dflt.configuration.ToSQL.IfExists.replace,
        index=dflt.configuration.ToSQL.Index.no,
        chunksize=dflt.configuration.ToSQL.rows_per_data_frame
    )
