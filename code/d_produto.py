import utilities as utl


def get(conn_input):
    return utl.convert_table_to_dataframe(
        conn_input=conn_input,
        schema_name="stage",
        table_name="STG_PRODUTO",
        columns=[
            "id_produto",
            "cod_barra",
            "nome_produto",
            "data_cadastro",
            "ativo"
        ]
    )


def treat(frame):
    columns_rename = {
        "id_produto": "CD_PRODUTO",
        "cod_barra": "CD_BARRAS",
        "nome_produto": "NO_PRODUTO",
        "data_cadastro": "DT_CADASTRO",
        "ativo": "FL_ATIVO"
    }

    frame_res = frame.assign(
        id_produto=lambda df: utl.convert_column_cpf_to_int64(
            column_data_frame=df.id_produto,
            default=-3
        ),
        cod_barra=lambda df: utl.convert_column_cpf_to_int64(
            column_data_frame=df.cod_barra,
            default=-3
        ),
        nome_produto=lambda df: utl.convert_column_to_tittle(
            column_data_frame=df.nome_produto
        ),
        data_cadastro=lambda df: utl.convert_column_to_date(
            column_data_frame=df.data_cadastro,
            format_="%d%m%Y",
            default="01011900"
        ),
        ativo=lambda df: utl.convert_column_cpf_to_int64(
            column_data_frame=df.ativo,
            default=-3
        )
    )

    frame_res.insert(
        loc=0,
        column="SK_PRODUTO",
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
        name="D_PRODUTO",
        con=conn_input,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000
    )
