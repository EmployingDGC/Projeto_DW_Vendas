import DW_TOOLS as dwt


def run_stg_cliente(connection):
    """
    Carrega a stage cliente
    :param connection: conexão com o banco de dados
    :return: None
    """

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="CLIENTE",
        stg_name="stg_cliente",
        tbl_exists="replace"
    )


def run_stg_endereco(connection):
    """
    Carrega a stage endereco
    :param connection: conexão com o banco de dados
    :return: None
    """

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="ENDERECO",
        stg_name="stg_endereco",
        tbl_exists="replace"
    )


def run_stg_forma_pagamento(connection):
    """
    Carrega a stage forma_pagamento
    :param connection: conexão com o banco de dados
    :return: None
    """

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="FORMA_PAGAMENTO",
        stg_name="stg_forma_pagamento",
        tbl_exists="replace"
    )


def run_stg_funcionario(connection):
    """
    Carrega a stage funcionario
    :param connection: conexão com o banco de dados
    :return: None
    """

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="FUNCIONARIO",
        stg_name="stg_funcionario",
        tbl_exists="replace"
    )


def run_stg_item_venda(connection):
    """
    Carrega a stage item_venda
    :param connection: conexão com o banco de dados
    :return: None
    """

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="ITEM_VENDA",
        stg_name="stg_item_venda",
        tbl_exists="replace"
    )


def run_stg_loja(connection):
    """
    Carrega a stage loja
    :param connection: conexão com o banco de dados
    :return: None
    """

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="LOJA",
        stg_name="stg_loja",
        tbl_exists="replace"
    )


def run_stg_produto(connection):
    """
    Carrega a stage loja
    :param connection: conexão com o banco de dados
    :return: None
    """

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="PRODUTO",
        stg_name="stg_produto",
        tbl_exists="replace"
    )


def run_stg_venda(connection):
    """
    Carrega a stage venda
    :param connection: conexão com o banco de dados
    :return: None
    """

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="VENDA",
        stg_name="stg_venda",
        tbl_exists="replace"
    )
