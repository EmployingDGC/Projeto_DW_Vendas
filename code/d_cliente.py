import utilities as utl


def get(conn_input):
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name="stage",
        table_name="STG_CLIENTE",
        columns=[
            "id_cliente",
            "cpf",
            "nome"
        ]
    )


def treat(frame):
    columns_rename = {
        "cpf": "CD_CPF",
        "nome": "NO_CLIENTE"
    }

    order_columns = [
        "SK_CLIENTE",
        "CD_CPF",
        "DS_CPF",
        "NO_CLIENTE"
    ]

    return frame.drop_duplicates(
        subset=["cpf", "nome"]
    ).assign(
        nome=lambda df: utl.convert_column_to_tittle(df.nome),
        cpf=lambda df: utl.convert_column_cpf_to_int64(df.cpf, -3),
        DS_CPF=lambda df: utl.convert_int_cpf_to_format_cpf(df.cpf),
        SK_CLIENTE=lambda df: utl.create_index_dataframe(df, 1)
    ).rename(
        columns=columns_rename
    ).pipe(
        func=utl.insert_default_values_table
    ).filter(
        items=order_columns
    )


def run(conn_input):
    utl.create_schema(conn_input, "dw")

    get(conn_input).pipe(
        func=treat
    ).to_sql(
        name="D_CLIENTE",
        con=conn_input,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000
    )
