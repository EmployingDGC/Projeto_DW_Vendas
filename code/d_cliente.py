import utilities as utl


def get(conn_input):
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name="stage",
        table_name="STG_CLIENTE",
        columns=[
            "cpf",
            "nome"
        ]
    )


def treat(frame):
    columns_rename = {
        "cpf": "CD_CPF",
        "nome": "NO_CLIENTE"
    }

    frame_res = frame.drop_duplicates(
        subset=["cpf", "nome"]
    ).assign(
        nome=lambda df: utl.convert_column_to_tittle(df.nome),
        cpf=lambda df: utl.convert_column_cpf_to_int64(df.cpf, -3)
    )

    frame_res.insert(
        loc=0,
        column="SK_CLIENTE",
        value=utl.create_index_dataframe(
            data_frame=frame_res,
            first_index=1
        )
    )

    frame_res.insert(
        loc=2,
        column="DS_CPF",
        value=utl.convert_int_cpf_to_format_cpf(frame_res.cpf)
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
        name="D_CLIENTE",
        con=conn_input,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000
    )
