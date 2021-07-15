import utilities as utl
import DW_TOOLS as dwt


# def run(conn_input):
#     get(conn_input).to_sql(
#         name="STG_ITEM_VENDA",
#         con=conn_input,
#         schema="stage",
#         if_exists="replace",
#         index=False,
#         chunksize=10000
#     )


def load_stg_item_venda(conn_input):
    utl.create_schema(conn_input, "stage")

    dwt.create_stage(
        conn_input=conn_input,
        conn_output=conn_input,
        schema_in="public",
        table="ITEM_VENDA",
        stg_name="STG_ITEM_VENDA",
        tbl_exists="replace"
    )
