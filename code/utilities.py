from sqlalchemy.engine.mock import MockConnection

import numpy as np
import pandas as pd


def convert_two_list_in_dict(keys: list,
                             values: list,
                             is_df: bool = False) -> dict:
    res = {}
    for key in keys:
        for value in values:
            if is_df:
                res[key] = [value]

            else:
                res[key] = value

            values.remove(value)

            break

    return res


def create_index_dataframe(data_frame: pd.DataFrame,
                           first_index: int = 0) -> list[int]:
    return [i + first_index for i in range(data_frame.shape[0])]


def convert_table_to_dataframe(conn_input: MockConnection,
                               schema_name: str,
                               table_name: str,
                               columns: list[str] = None,
                               qty_parts: int = 100) -> pd.DataFrame:
    str_columns = ""

    if not columns or len(columns) == 0:
        str_columns = "*"

    else:
        for col in columns:
            str_columns += f"\"{col}\", "

        str_columns = str_columns[:-2]

    select = conn_input.execute(
        f"select {str_columns} "
        f"from \"{schema_name.lower()}\".\"{table_name}\" "
    )

    if qty_parts <= 0:
        qty_parts = 100

    data_frame = pd.DataFrame()

    select_list = [x for x in select]

    qty_rows = len(select_list)

    qty_rows_per_dataframe = qty_rows // qty_parts
    qty_over_rows = qty_rows % qty_parts

    start = 0
    end = qty_rows_per_dataframe

    if end < 1:
        qty_parts = 1
        qty_over_rows = 0
        end = qty_rows

    for i in range(qty_parts):
        data_frame = pd.concat([
            data_frame,
            pd.DataFrame(
                select_list[start:end],
                columns=select.keys()
            )
        ])

        start += qty_rows_per_dataframe
        end += qty_rows_per_dataframe

    if qty_over_rows > 0:
        data_frame = pd.concat([
            data_frame,
            pd.DataFrame(
                select_list[start:end],
                columns=select.keys(),
            )
        ])

    return data_frame.reset_index(drop=True)


def convert_column_to_float64(column_data_frame: pd.Series,
                              default: float) -> pd.Series:
    return column_data_frame.apply(
        lambda num:
        float(num)
        if str(num).isnumeric() else
        float(str(num).replace(",", "."))
        if str(num).replace(",", ".").replace(".", "").isnumeric()
        and str(num).replace(",", ".").count(".") == 1 else
        float(default)
    )


def convert_column_to_int64(column_data_frame: pd.Series,
                            default: int) -> pd.Series:
    return column_data_frame.apply(
        lambda num:
        int(num)
        if str(num).isnumeric() else
        int(str(num).replace(",", ".").split(".")[0])
        if str(num).replace(",", ".").replace(".", "").isnumeric()
        and str(num).replace(",", ".").count(".") == 1 else
        int(default)
    )


def convert_column_to_date(column_data_frame: pd.Series,
                           format_: str,
                           default: str) -> pd.Series:
    return pd.to_datetime(
        arg=column_data_frame.apply(
            lambda date:
            f"{str(date).replace('/', '').replace('-', '').strip()}"
            if len(str(date).replace('/', '').replace('-', '').strip()) == 8 else
            default
        ),
        format=format_
    )


def convert_column_to_tittle(column_data_frame: pd.Series) -> pd.Series:
    return column_data_frame.apply(
        lambda value:
        str(value).strip().title()
    )


def convert_column_to_upper(column_data_frame: pd.Series) -> pd.Series:
    return column_data_frame.apply(
        lambda value:
        str(value).strip().upper()
    )


def convert_column_cpf_to_int64(column_data_frame: pd.Series,
                                default: int) -> pd.Series:
    return column_data_frame.apply(
        lambda cpf:
        int(str(cpf).replace("-", "").replace(".", "").replace(" ", "").strip())
        if str(cpf).replace("-", "").replace(".", "").replace(" ", "").strip().isnumeric()
        and len(str(cpf).replace("-", "").replace(".", "").replace(" ", "").strip()) == 11 else
        int(default)
    )


def convert_int_cpf_to_format_cpf(column_data_frame: pd.Series) -> pd.Series:
    return column_data_frame.apply(
        lambda cpf:
        f"{int(str(f'{int(cpf):011}')[:3]):03}"
        f".{int(str(f'{int(cpf):011}')[3:6]):03}"
        f".{int(str(f'{int(cpf):011}')[6:9]):03}"
        f"-{int(str(f'{int(cpf):011}')[9:]):02}"
    )


def convert_column_cnpj_to_int64(column_data_frame: pd.Series,
                                 default: int) -> pd.Series:
    return column_data_frame.apply(
        lambda cnpj:
        int(str(cnpj).replace("-", "").replace(".", "").replace("/", "").replace(" ", "").strip())
        if str(cnpj).replace("-", "").replace(".", "").replace("/", "").replace(" ", "").strip().isnumeric()
        and len(str(cnpj).replace("-", "").replace(".", "").replace("/", "").replace(" ", "").strip()) == 14 else
        int(default)
    )


def convert_int_cnpj_to_format_cnpj(column_data_frame: pd.Series) -> pd.Series:
    return column_data_frame.apply(
        lambda cnpj:
        f"{int(str(f'{int(cnpj):011}')[:2]):02}"
        f".{int(str(f'{int(cnpj):011}')[2:5]):03}"
        f".{int(str(f'{int(cnpj):011}')[5:8]):03}"
        f"/{int(str(f'{int(cnpj):011}')[8:12]):04}"
        f"-{int(str(f'{int(cnpj):011}')[12:]):02}"
    )


def convert_column_datetime_to_hour(column_data_frame: pd.Series,
                                    default: int) -> pd.Series:
    return column_data_frame.apply(
        lambda date:
        int(str(date).split(" ")[1].split(":")[0])
        if str(date).split(" ")[1].split(":")[0].isnumeric() else
        int(default)
    )


def convert_column_datetime_to_day(column_data_frame: pd.Series,
                                   default: int) -> pd.Series:
    return column_data_frame.apply(
        lambda date:
        int(str(date).split(" ")[0].split("-")[2])
        if str(date).split(" ")[0].split("-")[2].isnumeric() else
        int(default)
    )


def insert_row(df: pd.DataFrame,
               row: int,
               values: list) -> pd.DataFrame:
    return df.iloc[:row, ].append(
        other=pd.DataFrame(
            data=convert_two_list_in_dict(
                keys=[k for k in df.keys()],
                values=values,
                is_df=True
            )
        ),
        ignore_index=True
    ).append(
        other=df.iloc[row:, ],
        ignore_index=True
    ).reset_index(drop=True)


def insert_default_values_table(df: pd.DataFrame) -> pd.DataFrame:
    return df.pipe(
        func=insert_row,
        row=0,
        values=[
            -3
            if f"{df[k].dtype}" == "int64" else
            -3.0
            if f"{df[k].dtype}" == "float64" else
            "1900-01-01 00:00:00"
            if f"{df[k].dtype}" == "datetime64[ns]" else
            "Desconhecido"
            for k in df.keys()
        ]
    ).pipe(
        func=insert_row,
        row=0,
        values=[
            -2
            if df[k].dtype == np.int64 else
            -2.0
            if df[k].dtype == np.float64 else
            "1900-01-01 00:00:00"
            if f"{df[k].dtype}" == "datetime64[ns]" else
            "Não Aplicável"
            for k in df.keys()
        ]
    ).pipe(
        func=insert_row,
        row=0,
        values=[
            -1
            if df[k].dtype == np.int64 else
            -1.0
            if df[k].dtype == np.float64 else
            "1900-01-01 00:00:00"
            if f"{df[k].dtype}" == "datetime64[ns]" else
            "Não Informado"
            for k in df.keys()
        ]
    )


def create_table(conn_output: MockConnection,
                 schema_name: str,
                 table_name: str,
                 table_vars: dict[str, str]) -> None:
    str_vars = ""

    for k, v in table_vars.items():
        str_vars += f"{k} {v}, "

    str_vars = str_vars[:-2]

    conn_output.execute(f"create table if not exists \"{schema_name.lower()}\".\"{table_name}\" ({str_vars})")


def create_schema(database: MockConnection,
                  schema_name: str) -> None:
    database.execute(f" create schema if not exists {schema_name.lower()}")


def drop_tables(conn_output: MockConnection,
                schema_name: str,
                dimensions_names: list[str]) -> None:
    for i in range(len(dimensions_names)):
        conn_output.execute(
            f" drop table if exists \"{schema_name.lower()}\".\"{dimensions_names[i]}\""
        )


def drop_schemas(conn_output: MockConnection,
                 schemas_names: list[str]) -> None:
    for schema in schemas_names:
        conn_output.execute(f" drop schema if exists {schema.lower()}")
