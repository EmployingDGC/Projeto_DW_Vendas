from sqlalchemy.engine.mock import MockConnection

import pandas as pd
import default as dflt


def get() -> pd.DataFrame:
    return pd.DataFrame(
        data={
            dflt.Dimension.Categoria.sk: [c for c in dflt.Category.all_categories],
            dflt.Dimension.Categoria.ds: [i + 1 for i in range(len(dflt.Category.all_categories))]
        }
    )


# def treat() -> pd.DataFrame:
#     pass


def create(conn_input: MockConnection) -> None:
    get().to_sql(
        name=dflt.TablesDW.categoria,
        con=conn_input,
        schema=dflt.Schema.dw,
        if_exists="replace",
        index=False,
        chunksize=dflt.Configuration.rows_per_data_frame
    )
