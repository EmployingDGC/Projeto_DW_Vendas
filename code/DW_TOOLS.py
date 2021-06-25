import pandas as pd


def merge_input(left, right, left_on, right_on, surrogate_key, suff):
    dict_na = right.query(
        f"{surrogate_key} == -3"
    ).to_dict('index')

    return left.merge(
        right=right,
        how="left",
        left_on=left_on,
        right_on=right_on,
        suffixes=suff
    ).fillna(dict_na[list(dict_na)[0]])


def create_stage(conn_input, conn_output, schema_in, table, stg_name, tbl_exists):
    read_table(
        conn=conn_input,
        schema=schema_in,
        table_name=table
    ).to_sql(
        name=stg_name,
        con=conn_output,
        schema="STAGE",
        if_exists=tbl_exists,
        index=False
    )


def dict_to_str(dict_):
    return [f'{k}" AS "{v}' for k, v in dict_.items()]


def concat_cols(str_):
    if isinstance(str_, dict):
        str_ = dict_to_str(str_)

    return '", "'.join(str_)


def read_table(conn, schema, table_name, columns=None, where=None, distinct=False):
    if distinct:
        distinct_clause = "DISTINCT"
    else:
        distinct_clause = ""

    if where is None:
        where_clause = ""
    else:
        where_clause = f"WHERE {where}"

    if columns is None:
        columns = "*"
    else:
        columns = f'"{concat_cols(columns)}"'

    query = f'SELECT {distinct_clause} {columns} FROM "{schema}"."{table_name}" {where_clause}'

    response = pd.read_sql_query(query, conn)

    return response
