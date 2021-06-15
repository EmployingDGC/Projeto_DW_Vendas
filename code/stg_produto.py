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
            schema_name="stage",
            table_name="STG_PRODUTO"
        )
    except:
        return frame

    filter_x = [
        "id_produto",
        "nome_produto_x",
        "cod_barra_x",
        "preco_custo_x",
        "percentual_lucro_x",
        "data_cadastro_x",
        "ativo_x"
    ]

    filter_y = [
        "id_produto",
        "nome_produto_y",
        "cod_barra_y",
        "preco_custo_y",
        "percentual_lucro_y",
        "data_cadastro_y",
        "ativo_y"
    ]

    rename_x = {
        "nome_produto_x": "nome_produto",
        "cod_barra_x": "cod_barra",
        "preco_custo_x": "preco_custo",
        "percentual_lucro_x": "percentual_lucro",
        "data_cadastro_x": "data_cadastro",
        "ativo_x": "ativo"
    }

    rename_y = {
        "nome_produto_y": "nome_produto",
        "cod_barra_y": "cod_barra",
        "preco_custo_y": "preco_custo",
        "percentual_lucro_y": "percentual_lucro",
        "data_cadastro_y": "data_cadastro",
        "ativo_y": "ativo"
    }

    modified_frame = df_current.merge(
        right=frame,
        how="right",
        on="id_produto"
    ).assign(
        data_cadastro_x=lambda df: utl.convert_column_to_str(
            utl.convert_column_to_date(df.data_cadastro_x, "%d%m%Y", "01011900")
        ),
        data_cadastro_y=lambda df: utl.convert_column_to_str(
            utl.convert_column_to_date(df.data_cadastro_y, "%d%m%Y", "01011900")
        )
    ).pipe(
        lambda df: df[~utl.compare_two_columns(df.data_cadastro_x, df.data_cadastro_y)]
    ).assign(
        FL_NEW=lambda df: df.data_cadastro_x.apply(
            lambda value: value == "1900-01-01 00:00:00"
        )
    )

    new_produtcs = modified_frame.pipe(
        lambda df: df[df.FL_NEW]
    ).filter(filter_y).rename(rename_y)

    update_products = modified_frame.pipe(
        lambda df: df[~df.FL_NEW]
    )

    # print(modified_frame)
    print("\n" + "-" * 100 + "\n")
    print(new_produtcs)
    print("\n" + "-" * 100 + "\n")
    print(update_products)

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
