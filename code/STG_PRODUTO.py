import utilities as utl
import connection as conn
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
            table_name="stg_produto"
        )
    except:
        return frame

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

    teste = frame.merge(
        right=stg_produto,
        how="left",
        on="id_produto"
    ).assign(
        fl_new=lambda df: df.apply(
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

    df_updated = teste[~teste.fl_new].assign(
        fl_trash=lambda df: df.apply(
            lambda row: (
                str(row.nome_produto_y) == str(row.nome_produto_x)
                and str(row.cod_barra_y) == str(row.cod_barra_x)
                and str(row.preco_custo_y) == str(row.preco_custo_x)
                and str(row.percentual_lucro_y) == str(row.percentual_lucro_x)
            ),
            axis=1
        )
    ).pipe(
        lambda df: df[~df.fl_trash]
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
        teste[teste.fl_new].filter(filter_x).rename(columns=rename_x)
    ])

    df_final.drop_duplicates(
        subset=["id_produto"]
    ).apply(
        lambda row: utl.delete_register_from_table(
            conn_output=conn_input,
            schema_name="stage",
            table_name="stg_produto",
            where=f"id_produto = {row.id_produto}"
        ),
        axis=1
    )

    return df_final


def load_stg_produto(frame, connection):
    """
    Carrega a slow change stage produto
    :param connection: conexão com o banco de dados do cliente
    :return: None
    """

    frame.to_sql(
        name="sgt_produto",
        con=connection,
        schema="stage",
        if_exists="append",
        index=False,
        chunksize=10000
    )


def run_stg_produto(connection):
    extract_stg_produto(connection).pipe(
        func=treat_stg_produto,
        connection=connection
    ).pipe(
        func=load_stg_produto,
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

    run_stg_produto(conn_db)
