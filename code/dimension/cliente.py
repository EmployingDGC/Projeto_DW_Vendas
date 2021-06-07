from sqlalchemy.engine.mock import MockConnection

import pandas as pd
import default as dflt


def get() -> pd.DataFrame:
    pass


def treat() -> pd.DataFrame:
    pass


def create(conn_input: MockConnection) -> None:
    get().to_sql(
        name=dflt.tables.dw.cliente,
        con=conn_input,
        schema=dflt.schema.dw,
        if_exists=dflt.configuration.ToSQL.IfExists.replace,
        index=dflt.configuration.ToSQL.Index.no,
        chunksize=dflt.configuration.ToSQL.rows_per_data_frame
    )
