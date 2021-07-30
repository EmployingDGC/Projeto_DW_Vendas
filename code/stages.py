import utilities as utl
import DW_TOOLS as dwt


def load_stg_cliente(connection):
    """
    Carrega a stage cliente
    :param connection: conexão com o banco de dados do cliente
    :return: None
    """

    utl.create_schema(connection, "stage")

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="CLIENTE",
        stg_name="STG_CLIENTE",
        tbl_exists="replace"
    )


def load_stg_forma_pagamento(connection):
    """
    Carrega a stage forma_pagamento
    :param connection: conexão com o banco de dados do cliente
    :return: None
    """

    utl.create_schema(connection, "stage")

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="FORMA_PAGAMENTO",
        stg_name="STG_FORMA_PAGAMENTO",
        tbl_exists="replace"
    )


def load_stg_funcionario(connection):
    """
    Carrega a stage funcionario
    :param connection: conexão com o banco de dados do cliente
    :return: None
    """

    utl.create_schema(connection, "stage")

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="FUNCIONARIO",
        stg_name="STG_FUNCIONARIO",
        tbl_exists="replace"
    )


def load_stg_item_venda(connection):
    """
    Carrega a stage item_venda
    :param connection: conexão com o banco de dados do cliente
    :return: None
    """

    utl.create_schema(connection, "stage")

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="ITEM_VENDA",
        stg_name="STG_ITEM_VENDA",
        tbl_exists="replace"
    )


def load_stg_venda(connection):
    """
    Carrega a stage venda
    :param connection: conexão com o banco de dados do cliente
    :return: None
    """

    utl.create_schema(connection, "stage")

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="VENDA",
        stg_name="STG_VENDA",
        tbl_exists="replace"
    )
