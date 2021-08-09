import pandas as pd

import utilities as utl
import DW_TOOLS as dwt
import connection as conn

from datetime import datetime


def extract_stg_loja(connection):
    """
    Extrai os dados para fazer a slow change stage loja
    :param connection: conexão com o banco de dados do cliente
    :return: dataframe com os dados extraidos para fazer a stage loja
    """

    return dwt.read_table(
        conn=connection,
        schema="public",
        table_name="LOJA"
    )


def treat_stg_loja(frame, connection):
    """
    Trata os dados para fazer a slow change stage loja
    :param frame: dataframe com os dados extraidos
    :param connection: conexão com o banco de daos das stages
    :return: dataframe com os dados tratados
    """

    try:
        df_current = dwt.read_table(
            conn=connection,
            schema="stage",
            table_name="stg_loja"
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
            fl_new=lambda df: df.apply(
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
            lambda df: df[df.fl_new].filter(select_columns_y).rename(columns=rename_y)
        ),
        modified_frame.pipe(
            lambda df: df[~df.fl_new].assign(
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
        fl_ativo=lambda df: df.ativo.apply(
            lambda value: value == 1
        ),
        data_final=lambda df: df.data_final.astype("datetime64[ns]"),
        data_inicial=lambda df: df.data_inicial.astype("datetime64[ns]")
    )

    stores.pipe(
        lambda df: df[~df.fl_ativo]
    ).apply(
        lambda row: utl.delete_register_from_table(
            conn_output=connection,
            schema_name="stage",
            table_name="stg_loja",
            where=f"id_loja = {row.id_loja}"
        ),
        axis=1
    )

    return stores.filter(select_columns).drop_duplicates(subset=columns_drop_duplicates)


def load_stg_loja(frame, connection):
    """
    Carrega a slow change stage loja
    :param frame: data frame a stage loja
    :param connection: conexão com o banco de dados do cliente
    :return: None
    """

    frame.to_sql(
        name="stg_loja",
        con=connection,
        schema="stage",
        if_exists="append",
        index=False,
        chunksize=10000
    )


def run_stg_loja(connection):
    extract_stg_loja(connection).pipe(
        func=treat_stg_loja,
        connection=connection
    ).pipe(
        func=load_stg_loja,
        connection=connection
    )


if __name__ == "__main__":
    conn_db = conn.create_connection_postgre(
        server="10.0.0.105",
        database="projeto_dw_vendas",
        username="postgres",
        password="itix.123",
        port=5432
    )

    run_stg_loja(conn_db)
