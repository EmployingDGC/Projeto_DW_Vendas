import utilities as utl


def get(conn_input):
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name="stage",
        table_name="STG_FORMA_PAGAMENTO",
        columns=[
            "nome",
            "descricao"
        ]
    )


def treat(frame):
    columns_rename = {
        "nome": "NO_PAGAMENTO",
        "descricao": "DS_PAGAMENTO"
    }

    frame_res = frame.drop_duplicates(
        subset=["nome", "descricao"]
    ).assign(
        nome=lambda df: utl.convert_column_to_upper(df.nome),
        descricao=lambda df: utl.convert_column_to_upper(df.descricao)
    )

    frame_res.insert(
        loc=0,
        column="SK_PAGAMENTO",
        value=utl.create_index_dataframe(
            data_frame=frame_res,
            first_index=1
        )
    )

    return frame_res.rename(
        columns=columns_rename
    ).pipe(
        func=utl.insert_default_values_table
    )


def run(conn_input):
    utl.create_schema(conn_input, "dw")

    get(conn_input).pipe(
        func=treat
    ).to_sql(
        name="D_TIPO_PAGAMENTO",
        con=conn_input,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000
    )
