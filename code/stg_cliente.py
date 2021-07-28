import utilities as utl
import DW_TOOLS as dwt


# def get(conn_input):
#     return utl.convert_table_to_dataframe(
#         conn_input=conn_input,
#         schema_name="public",
#         table_name="CLIENTE"
#     )


# def run(conn_input):
#     get(conn_input).to_sql(
#         name="STG_CLIENTE",
#         con=conn_input,
#         schema="stage",
#         if_exists="replace",
#         index=False,
#         chunksize=10000
#     )


def load_stg_cliente(connection):
    utl.create_schema(connection, "stage")

    dwt.create_stage(
        conn_input=connection,
        conn_output=connection,
        schema_in="public",
        table="CLIENTE",
        stg_name="STG_CLIENTE",
        tbl_exists="replace"
    )
