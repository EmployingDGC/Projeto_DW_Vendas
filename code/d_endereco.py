import utilities as utl


def get(conn_input):
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name="stage",
        table_name="STG_ENDERECO",
        columns=[
            "estado",
            "cidade",
            "bairro",
            "rua"
        ]
    )


def treat(frame):
    columns_rename = {
        "estado": "NO_ESTADO",
        "cidade": "NO_CIDADE",
        "bairro": "NO_BAIRRO",
        "rua": "NO_RUA"
    }

    frame_res = frame.assign(
        estado=lambda df: utl.convert_column_to_tittle(df.estado),
        cidade=lambda df: utl.convert_column_to_tittle(df.cidade),
        bairro=lambda df: utl.convert_column_to_tittle(df.bairro),
        rua=lambda df: utl.convert_column_to_tittle(df.rua)
    )

    frame_res.insert(
        loc=0,
        column="SK_ENDERECO",
        value=utl.create_index_dataframe(
            data_frame=frame_res,
            first_index=1
        )
    )

    return frame_res.drop_duplicates(
        subset=[k for k in frame_res.keys()]
    ).rename(
        columns=columns_rename
    ).pipe(
        func=utl.insert_default_values_table
    )


def run(conn_input):
    utl.create_schema(conn_input, "dw")

    get(conn_input).pipe(
        func=treat
    ).to_sql(
        name="D_ENDERECO",
        con=conn_input,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000
    )
