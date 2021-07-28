import utilities as utl
import DW_TOOLS as dwt


# def run(conn_input):
#     get(conn_input).to_sql(
#         name="STG_VENDA",
#         con=conn_input,
#         schema="stage",
#         if_exists="replace",
#         index=False,
#         chunksize=10000
#     )


def load_stg_venda(connection):
    utl.create_schema(connection, "stage")

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="VENDA",
        stg_name="STG_VENDA",
        tbl_exists="replace"
    )
