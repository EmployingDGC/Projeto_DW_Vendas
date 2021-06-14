import utilities as utl

import pandas as pd


def get(conn_input):
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name="public",
        table_name="PRODUTO"
    )


def treat(frame: pd.DataFrame, conn_input):
    try:
        df_current = utl.convert_table_to_dataframe(
            conn_input=conn_input,
            schema_name="dw",
            table_name="D_PRODUTO"
        )
    except:
        return frame

    join_frame = df_current.merge(
        right=frame,
        how="left",
        left_on="CD_PRODUTO",
        right_on="id_produto"
    ).iloc[3:].assign(
        id_produto=lambda df: utl.convert_column_to_int64(df.id_produto, -3),
        cod_barra=lambda df: utl.convert_column_to_int64(df.cod_barra, -3),
        preco_custo=lambda df: utl.convert_column_to_float64(df.preco_custo, -3),
        percentual_lucro=lambda df: utl.convert_column_to_float64(df.percentual_lucro, -3),
        data_cadastro=lambda df: utl.convert_column_to_date(df.data_cadastro, "%d%m%Y", "01011900"),
        nome_produto=lambda df: utl.convert_column_to_tittle(df.nome_produto),
        FL_NEW=lambda df: df.isnull().id_produto
    )

    frame_new = [join_frame[~join_frame.FL_NEW].iloc[i] for i in range(join_frame.shape[0])]
    join_frame[~join_frame.FL_NEW].apply(
        lambda row:
        pd.concat([frame_new, row]),
        axis=1
    )

    print(frame_new)

def run(conn_input):
    get(conn_input).pipe(
        func=treat,
        conn_input=conn_input
    ).to_sql(
        name="STG_PRODUTO",
        con=conn_input,
        schema="stage",
        if_exists="append",
        index=False,
        chunksize=10000
    )
