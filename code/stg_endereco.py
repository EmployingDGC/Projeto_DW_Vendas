import utilities as utl


def get(conn_input):
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name="public",
        table_name="ENDERECO"
    )


def run(conn_input):
    get(conn_input).to_sql(
        name="STG_ENDERECO",
        con=conn_input,
        schema="stage",
        if_exists="replace",
        index=False,
        chunksize=10000
    )
