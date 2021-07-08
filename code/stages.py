import dw_tools as dwt
import utilities as utl


def stg_cliente(connection):
    utl.create_schema(
        database=connection,
        schema_name="stage"
    )

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="CLIENTE",
        stg_name="STG_CLIENTE",
        tbl_exists="replace"
    )


def stg_endereco(connection):
    utl.create_schema(
        database=connection,
        schema_name="stage"
    )

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="ENDERECO",
        stg_name="STG_ENDERECO",
        tbl_exists="replace"
    )


def stg_forma_pagamento(connection):
    utl.create_schema(
        database=connection,
        schema_name="stage"
    )

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="FORMA_PAGAMENTO",
        stg_name="STG_FORMA_PAGAMENTO",
        tbl_exists="replace"
    )


def stg_funcionario(connection):
    utl.create_schema(
        database=connection,
        schema_name="stage"
    )

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="FUNCIONARIO",
        stg_name="STG_FUNCIONARIO",
        tbl_exists="replace"
    )


def stg_item_venda(connection):
    utl.create_schema(
        database=connection,
        schema_name="stage"
    )

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="ITEM_VENDA",
        stg_name="STG_ITEM_VENDA",
        tbl_exists="replace"
    )


def stg_loja(connection):
    utl.create_schema(
        database=connection,
        schema_name="stage"
    )

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="LOJA",
        stg_name="STG_LOJA",
        tbl_exists="replace"
    )


def stg_produto(connection):
    utl.create_schema(
        database=connection,
        schema_name="stage"
    )

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="PRODUTO",
        stg_name="STG_PRODUTO",
        tbl_exists="replace"
    )


def stg_venda(connection):
    utl.create_schema(
        database=connection,
        schema_name="stage"
    )

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="VENDA",
        stg_name="STG_VENDA",
        tbl_exists="replace"
    )
