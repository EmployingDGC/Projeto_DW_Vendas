import utilities as utl

import pandas as pd


def get(conn_input):
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name="public",
        table_name="PRODUTO"
    )


def treat(frame, conn_input):
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

    filter_drop = [
        "id_produto",
        "nome_produto",
        "cod_barra",
        "preco_custo",
        "percentual_lucro",
        "data_cadastro"
    ]

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

    if modified_frame.empty:
        return modified_frame

    new_produtcs = pd.concat([
        modified_frame.pipe(
            lambda df: df[df.FL_NEW]
        ).filter(filter_y).rename(columns=rename_y),
        modified_frame.pipe(
            lambda df: df[~df.FL_NEW]
        ).pipe(
            lambda df: df.apply(
                lambda row: utl.set_ativo(row),
                axis=1
            )
        ).pipe(
            lambda df: pd.concat([
                df.filter(filter_x).rename(columns=rename_x),
                df.filter(filter_y).rename(columns=rename_y)
            ])
        )
    ]).assign(
        FL_ATIVO=lambda df: df.ativo.apply(
            lambda value: int(value) == 1
        ),
        data_cadastro=lambda df: utl.convert_column_datetime_to_date(df.data_cadastro).apply(
            lambda row: f"{str(row).split('-')[2]}/{str(row).split('-')[1]}/{str(row).split('-')[0]}"
        )
    ).pipe(
        lambda df: df.drop_duplicates(filter_drop)
    )

    new_produtcs.pipe(
        lambda df: df[~df.FL_ATIVO]
    ).apply(
        lambda row: utl.delete_register_from_table(
            conn_output=conn_input,
            schema_name="stage",
            table_name="STG_PRODUTO",
            where=f"id_produto = {row.id_produto}"
        ),
        axis=1
    )

    return new_produtcs.drop(columns=["FL_ATIVO"])


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
