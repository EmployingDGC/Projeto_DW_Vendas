import utilities as utl
import DW_TOOLS as dwt


def extract_stg_venda(conn_input):
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name="public",
        table_name="VENDA"
    )


# def run(conn_input):
#     get(conn_input).to_sql(
#         name="STG_VENDA",
#         con=conn_input,
#         schema="stage",
#         if_exists="replace",
#         index=False,
#         chunksize=10000
#     )


def run(conn_input):
    utl.create_schema(conn_input, "stage")

    dwt.create_stage(
        conn_input=conn_input,
        conn_output=conn_input,
        schema_in="public",
        table="VENDA",
        stg_name="STG_VENDA",
        tbl_exists="replace"
    )
