from sqlalchemy.engine.mock import MockConnection

import pandas as pd
import default as dflt


def get() -> pd.DataFrame:
    return pd.DataFrame(
        data={
            dflt.dimensions.categoria.sk: [c for c in dflt.categoria.all_categories],
            dflt.dimensions.categoria.ds: [i + 1 for i in range(len(dflt.categoria.all_categories))]
        }
    )


# def treat() -> pd.DataFrame:
#     pass


def create(conn_input: MockConnection) -> None:
    get().to_sql(
        name=dflt.tables.dw.categoria,
        con=conn_input,
        schema=dflt.schema.dw,
        if_exists="replace",
        index=False,
        chunksize=dflt.configuration.rows_per_data_frame
    )
