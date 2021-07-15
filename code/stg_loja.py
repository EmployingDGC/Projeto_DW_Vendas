import pandas as pd

import utilities as utl
import DW_TOOLS as dwt

from datetime import datetime


# def get(conn_input):
#     return utl.convert_table_to_dataframe(
#         conn_input=conn_input,
#         schema_name="public",
#         table_name="LOJA"
#     )


def extract_stg_loja(conn_input):
    return dwt.read_table(
        conn=conn_input,
        schema="public",
        table_name="LOJA"
    )


def treat_stg_loja(frame, conn_output):
    try:
        # df_current = utl.convert_table_to_dataframe(
        #     conn_input=conn_output,
        #     schema_name="stage",
        #     table_name="STG_LOJA"
        # )

        df_current = dwt.read_table(
            conn=conn_output,
            schema="stage",
            table_name="STG_LOJA"
        )
    except:
        return frame.assign(
            data_inicial=lambda df: datetime.now().date(),
            data_final=lambda df: None,
            ativo=lambda df: 1
        ).assign(
            data_inicial=lambda df: df.data_inicial.astype("datetime64[ns]"),
            data_final=lambda df: df.data_final.astype("datetime64[ns]")
        )

    select_columns_x = [
        "id_loja",
        "nome_loja_x",
        "razao_social_x",
        "cnpj_x",
        "telefone_x",
        "id_endereco_x",
        "data_inicial_x",
        "data_final_x",
        "ativo_x"
    ]

    select_columns_y = [
        "id_loja",
        "nome_loja_y",
        "razao_social_y",
        "cnpj_y",
        "telefone_y",
        "id_endereco_y",
        "data_inicial_y",
        "data_final_y",
        "ativo_y"
    ]

    columns_drop_duplicates = [
        "id_loja",
        "nome_loja",
        "razao_social",
        "cnpj",
        "telefone",
        "id_endereco"
    ]

    select_columns = [
        "id_loja",
        "nome_loja",
        "razao_social",
        "cnpj",
        "telefone",
        "id_endereco",
        "data_inicial",
        "data_final",
        "ativo"
    ]

    rename_x = {
        "nome_loja_x": "nome_loja",
        "razao_social_x": "razao_social",
        "cnpj_x": "cnpj",
        "telefone_x": "telefone",
        "id_endereco_x": "id_endereco",
        "data_inicial_x": "data_inicial",
        "data_final_x": "data_final",
        "ativo_x": "ativo"
    }

    rename_y = {
        "nome_loja_y": "nome_loja",
        "razao_social_y": "razao_social",
        "cnpj_y": "cnpj",
        "telefone_y": "telefone",
        "id_endereco_y": "id_endereco",
        "data_inicial_y": "data_inicial",
        "data_final_y": "data_final",
        "ativo_y": "ativo"
    }

    try:
        modified_frame = df_current.merge(
            right=frame,
            how="right",
            on="id_loja"
        ).rename(
            columns={"data_inicial": "data_inicial_x", "data_final": "data_final_x", "ativo": "ativo_x"}
        ).assign(
            data_inicial_x=lambda df: df.data_inicial_x.astype("datetime64[ns]"),
            data_inicial_y=lambda df: df.data_inicial_x.astype("datetime64[ns]"),
            data_final_y=lambda df: None,
            FL_TRASH=lambda df: df.apply(
                lambda row: (
                    row.nome_loja_x == row.nome_loja_y and
                    row.razao_social_x == row.razao_social_y and
                    row.cnpj_x == row.cnpj_y and
                    row.telefone_x == row.telefone_y and
                    row.id_endereco_x == row.id_endereco_y
                ),
                axis=1
            )
        ).pipe(
            lambda df: df[~df.FL_TRASH]
        ).assign(
            FL_NEW=lambda df: df.apply(
                lambda row: (
                        str(row.nome_loja_x) == "nan" and
                        str(row.razao_social_x) == "nan" and
                        str(row.cnpj_x) == "nan" and
                        str(row.telefone_x) == "nan" and
                        str(row.id_endereco_x) == "nan"
                ),
                axis=1
            ),
            ativo_y=lambda df: 1
        )
    except:
        return pd.DataFrame(columns=df_current.keys().tolist())

    stores = pd.concat([
        modified_frame.pipe(
            lambda df: df[df.FL_NEW].filter(select_columns_y).rename(columns=rename_y)
        ),
        modified_frame.pipe(
            lambda df: df[~df.FL_NEW].assign(
                data_final_x=lambda df1: df.data_inicial_y,
                ativo_x=lambda df1: 0
            )
        ).pipe(
            lambda df: pd.concat([
                df.filter(select_columns_x).rename(columns=rename_x),
                df.filter(select_columns_y).rename(columns=rename_y)
            ])
        )
    ]).assign(
        FL_ATIVO=lambda df: df.ativo.apply(
            lambda value: value == 1
        ),
        data_final=lambda df: df.data_final.astype("datetime64[ns]"),
        data_inicial=lambda df: df.data_inicial.astype("datetime64[ns]")
    )

    stores.pipe(
        lambda df: df[~df.FL_ATIVO]
    ).apply(
        lambda row: utl.delete_register_from_table(
            conn_output=conn_output,
            schema_name="stage",
            table_name="STG_LOJA",
            where=f"id_loja = {row.id_loja}"
        ),
        axis=1
    )

    return stores.filter(select_columns).drop_duplicates(subset=columns_drop_duplicates)


def run_stg_loja(conn_input):
    utl.create_schema(conn_input, "stage")

    extract_stg_loja(conn_input).pipe(
        func=treat_stg_loja,
        conn_output=conn_input
    ).to_sql(
        name="STG_LOJA",
        con=conn_input,
        schema="stage",
        if_exists="append",
        index=False,
        chunksize=10000
    )
