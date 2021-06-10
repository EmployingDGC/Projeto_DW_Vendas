import utilities as utl


def get(conn_input):
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name="stage",
        table_name="STG_LOJA",
        columns=[
            "id_loja",
            "id_endereco",
            "cnpj",
            "nome_loja",
            "razao_social"
        ]
    )


def treat(frame):
    columns_rename = {
        "cnpj": "CD_CNPJ",
        "nome_loja": "NO_LOJA",
        "razao_social": "NO_RAZAO_SOCIAL"
    }

    order_columns = [
        "SK_LOJA",
        "CD_CNPJ",
        "DS_CNPJ",
        "NO_LOJA",
        "NO_RAZAO_SOCIAL"
    ]

    return frame.assign(
        cnpj=lambda df: utl.convert_column_cnpj_to_int64(df.cnpj, -3),
        nome_loja=lambda df: utl.convert_column_to_tittle(df.nome_loja),
        razao_social=lambda df: utl.convert_column_to_tittle(df.razao_social),
        DS_CNPJ=lambda df: utl.convert_int_cnpj_to_format_cnpj(df.cnpj),
        SK_LOJA=lambda df: utl.create_index_dataframe(df, 1)
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
        name="D_LOJA",
        con=conn_input,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000
    )
