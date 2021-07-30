import utilities as utl
import DW_TOOLS as dwt

from sqlalchemy.types import (
    Integer,
    String,
    DateTime
)


def extract_dim_loja(connection):
    """
    Extrai os dados para fazer a dimensão loja
    :param connection: conexão com o banco de dados das stages
    :return: dataframe com o merge das stages loja e endereco
    """

    return dwt.read_table(
        conn=connection,
        schema="stage",
        table_name="STG_LOJA"
    ).merge(
        right=dwt.read_table(
            conn=connection,
            schema="stage",
            table_name="STG_ENDERECO"
        ),
        how="inner",
        on="id_endereco"
    )


def treat_dim_loja(frame):
    """
    Trata os dados extraidos para fazer a dimensão loja
    :param frame: dataframe com os dados extraidos
    :return: dataframe com os dados tratados para fazer a dimensão loja
    """

    columns_rename = {
        "id_loja": "CD_LOJA",
        "id_endereco": "CD_ENDERECO",
        "telefone": "NU_TELEFONE",
        "data_inicial": "DT_INICIAL",
        "data_final": "DT_FINAL",
        "ativo": "FL_ATIVO"
    }

    select_columns = [
        "SK_LOJA",
        "CD_LOJA",
        "CD_ENDERECO",
        "NU_CNPJ",
        "NU_TELEFONE",
        "NO_LOJA",
        "NO_RAZAO_SOCIAL",
        "NO_ESTADO",
        "NO_CIDADE",
        "NO_BAIRRO",
        "NO_RUA",
        "DT_INICIAL",
        "DT_FINAL",
        "FL_ATIVO"
    ]

    return frame.assign(
        NU_CNPJ=lambda df: df.cnpj.str.replace("-", "").str.strip(),
        NO_LOJA=lambda df: df.nome_loja.str.upper().str.strip(),
        NO_RAZAO_SOCIAL=lambda df: df.razao_social.str.upper().str.strip(),
        SK_LOJA=lambda df: utl.create_index_dataframe(df, 1),
        NO_ESTADO=lambda df: df.estado.str.upper().str.strip(),
        NO_CIDADE=lambda df: df.cidade.str.upper().str.strip(),
        NO_BAIRRO=lambda df: df.bairro.str.upper().str.strip(),
        NO_RUA=lambda df: df.rua.str.upper().str.strip()
    ).rename(
        columns=columns_rename
    ).assign(
        DT_INICIAL=lambda df: df.DT_INICIAL.astype("datetime64[ns]"),
        DT_FINAL=lambda df: df.DT_FINAL.astype("datetime64[ns]")
    ).pipe(
        func=utl.insert_default_values_table
    ).filter(
        items=select_columns
    )


def load_dim_loja(connection):
    """
    Carrega os dados tratados para fazer a dimensão loja
    :param connection: conexão com o banco de dados de saída
    :return: None
    """

    dtypes = {
        "SK_LOJA": Integer(),
        "CD_LOJA": Integer(),
        "CD_ENDERECO": Integer(),
        "NU_CNPJ": String(),
        "NU_TELEFONE": String(),
        "NO_LOJA": String(),
        "NO_RAZAO_SOCIAL": String(),
        "NO_ESTADO": String(),
        "NO_CIDADE": String(),
        "NO_BAIRRO": String(),
        "NO_RUA": String(),
        "DT_INICIAL": DateTime(),
        "DT_FINAL": DateTime(),
        "FL_ATIVO": Integer()
    }

    utl.create_schema(connection, "dw")

    extract_dim_loja(connection).pipe(
        func=treat_dim_loja
    ).to_sql(
        name="D_LOJA",
        con=connection,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000,
        dtype=dtypes
    )
