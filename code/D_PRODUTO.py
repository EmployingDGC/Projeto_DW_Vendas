import utilities as utl
import DW_TOOLS as dwt

from sqlalchemy.types import (
    Integer,
    String,
    DateTime
)


def extract_dim_produto(connection):
    """
    Extrai os dados para fazer a dimensão produto
    :param connection: conexão com o banco de dados das stages
    :return: dataframe com os dados extraidos da stage produto
    """

    return dwt.read_table(
        conn=connection,
        schema="stage",
        table_name="STG_PRODUTO",
        columns=[
            "id_produto",
            "cod_barra",
            "nome_produto",
            "data_cadastro",
            "ativo"
        ]
    )


def treat_dim_produto(frame):
    """
    Trata os dados extraidos para fazer a dimensão produto
    :param frame: dataframe com os dados extraidos
    :return: dataframe com os dados tratados para fazer a dimensão produto
    """

    columns_rename = {
        "id_produto": "CD_PRODUTO",
        "cod_barra": "CD_BARRAS",
        "nome_produto": "NO_PRODUTO",
        "data_cadastro": "DT_CADASTRO",
        "ativo": "FL_ATIVO"
    }

    order_columns = [
        "SK_PRODUTO",
        "CD_PRODUTO",
        "CD_BARRAS",
        "NO_PRODUTO",
        "DT_CADASTRO",
        "FL_ATIVO"
    ]

    return frame.assign(
        id_produto=lambda df: utl.convert_column_to_int64(df.id_produto, -3),
        cod_barra=lambda df: utl.convert_column_to_int64(df.cod_barra, -3),
        nome_produto=lambda df: utl.convert_column_to_tittle(df.nome_produto),
        data_cadastro=lambda df: utl.convert_column_to_date(df.data_cadastro, "%d%m%Y", "01011900"),
        ativo=lambda df: utl.convert_column_to_int64(df.ativo, -3),
        SK_PRODUTO=lambda df: utl.create_index_dataframe(df, 1)
    ).rename(
        columns=columns_rename
    ).pipe(
        func=utl.insert_default_values_table
    ).filter(
        items=order_columns
    )


def load_dim_produto(connection):
    """
    Carrega a dimensão produto no banco de dados
    :param connection: conexão com o banco de dados de saída
    :return: None
    """

    dtypes = {
        "SK_PRODUTO": Integer(),
        "CD_PRODUTO": Integer(),
        "CD_BARRAS": Integer(),
        "NO_PRODUTO": String(),
        "DT_CADASTRO": DateTime(),
        "FL_ATIVO": Integer()
    }

    utl.create_schema(connection, "dw")

    extract_dim_produto(connection).pipe(
        func=treat_dim_produto
    ).to_sql(
        name="D_PRODUTO",
        con=connection,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )
