import utilities as utl
import DW_TOOLS as dwt

import pandas as pd

from datetime import datetime


def extract_stg_produto(conn_input):
    """
    Extrai os dados para fazer a slow change stage produto
    :param connection: conexão com o banco de dados do cliente
    :return: dataframe com os dados extraidos para fazer a stage produto
    """

    return dwt.read_table(
        conn=conn_input,
        schema="public",
        table_name="PRODUTO"
    )


def treat_stg_produto(frame, conn_input):
    """
    Trata os dados para fazer a slow change stage produto
    :param frame: dataframe com os dados extraidos
    :param conn_output: conexão com o banco de daos das stages
    :return: dataframe com os dados tratados
    """

    try:
        stg_produto = dwt.read_table(
            conn=conn_input,
            schema="stage",
            table_name="STG_PRODUTO"
        )
    except:
        return frame.assign(
            data_cadastro=lambda df: df.data_cadastro.apply(
                lambda value: (
                    str(value)
                    if len(str(value)) == 10
                    else
                    "01/01/1900"
                )
            )
        )

    rename_x = {
        "nome_produto_x": "nome_produto",
        "cod_barra_x": "cod_barra",
        "preco_custo_x": "preco_custo",
        "percentual_lucro_x": "percentual_lucro",
        "data_cadastro_x": "data_cadastro",
        "ativo_x": "ativo"
    }

    filter_x = [
        "id_produto",
        "nome_produto_x",
        "cod_barra_x",
        "preco_custo_x",
        "percentual_lucro_x",
        "data_cadastro_x",
        "ativo_x"
    ]

    rename_y = {
        "nome_produto_y": "nome_produto",
        "cod_barra_y": "cod_barra",
        "preco_custo_y": "preco_custo",
        "percentual_lucro_y": "percentual_lucro",
        "data_cadastro_y": "data_cadastro",
        "ativo_y": "ativo"
    }

    filter_y = [
        "id_produto",
        "nome_produto_y",
        "cod_barra_y",
        "preco_custo_y",
        "percentual_lucro_y",
        "data_cadastro_y",
        "ativo_y"
    ]

    teste = frame.assign(
        data_cadastro=lambda df: df.data_cadastro.apply(
            lambda value: (
                str(value)
                if len(str(value)) == 10
                else
                "01/01/1900"
            )
        )
    ).merge(
        right=stg_produto.assign(
            data_cadastro=lambda df: df.data_cadastro.apply(
                lambda value: (
                    str(value)
                    if len(str(value)) == 10
                    else
                    "01/01/1900"
                )
            )
        ),
        how="left",
        on="id_produto"
    ).assign(
        FL_NEW=lambda df: df.apply(
            lambda row: (
                str(row.nome_produto_y)
                == str(row.cod_barra_y)
                == str(row.preco_custo_y)
                == str(row.percentual_lucro_y)
                == str(row.data_cadastro_y)
                == str(row.ativo_y)
                == "nan"
            ),
            axis=1
        )
    )

    df_updated = teste[~teste.FL_NEW].assign(
        cod_barra_y=lambda df: utl.convert_column_to_int64(
            column_data_frame=df.cod_barra_y,
            default=-3
        ),
        FL_TRASH=lambda df: df.apply(
            lambda row: (
                str(row.nome_produto_y) == str(row.nome_produto_x)
                and str(row.cod_barra_y) == str(row.cod_barra_x)
                and str(row.preco_custo_y) == str(row.preco_custo_x)
                and str(row.percentual_lucro_y) == str(row.percentual_lucro_x)
            ),
            axis=1
        )
    ).pipe(
        lambda df: df[~df.FL_TRASH]
    ).assign(
        ativo_x=lambda df: df.ativo_x.apply(
            lambda value: 1
        ),
        ativo_y=lambda df: df.ativo_x.apply(
            lambda value: 0
        ),
        data_cadastro_x=lambda df: datetime.now().strftime('%d/%m/%Y')
    )

    df_final = pd.concat([
        df_updated.filter(filter_y).rename(columns=rename_y),
        df_updated.filter(filter_x).rename(columns=rename_x),
        teste[teste.FL_NEW].filter(filter_x).rename(columns=rename_x)
    ])

    df_final.drop_duplicates(
        subset=["id_produto"]
    ).apply(
        lambda row: utl.delete_register_from_table(
            conn_output=conn_input,
            schema_name="stage",
            table_name="STG_PRODUTO",
            where=f"id_produto = {row.id_produto}"
        ),
        axis=1
    )

    return df_final


def load_stg_produto(connection):
    """
    Carrega a slow change stage produto
    :param connection: conexão com o banco de dados do cliente
    :return: None
    """

    utl.create_schema(connection, "stage")

    extract_stg_produto(connection).pipe(
        func=treat_stg_produto,
        conn_input=connection
    ).to_sql(
        name="STG_PRODUTO",
        con=connection,
        schema="stage",
        if_exists="append",
        index=False,
        chunksize=10000
    )
