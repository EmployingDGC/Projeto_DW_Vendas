import datetime as dt

import pandas as pd
import unidecode as ud


def convert_two_list_in_dict(keys,
                             values,
                             is_df=False):
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


def create_index_dataframe(data_frame,
                           first_index=0):
    first_index = int(first_index)

    return [i + first_index for i in range(data_frame.shape[0])]


def convert_table_to_dataframe(conn_input,
                               schema_name,
                               table_name,
                               columns=None,
                               qty_parts=100):
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


def convert_column_to_float64(column_data_frame,
                              default):
    def if_1(num):
        return str(num).isnumeric()

    def if_2(num):
        str_ = str(num).replace(",", ".")

        return (
            str_.count(".") == 1
            and
            str_.replace(".", "").isnumeric()
        )

    return column_data_frame.apply(
        lambda num:
        float(num)
        if if_1(num) else
        float(str(num).replace(",", "."))
        if if_2(num) else
        float(default)
    )


def convert_column_to_int64(column_data_frame,
                            default):
    def if_1(num):
        return str(num).isnumeric()

    def if_2(num):
        str_ = str(num).replace(",", ".")

        return (
            str_.count(".") == 1
            and
            str_.replace(".", "").isnumeric()
        )

    return column_data_frame.apply(
        lambda num:
        int(num)
        if if_1(num) else
        int(str(num).replace(",", ".").split(".")[0])
        if if_2(num) else
        int(default)
    )


def convert_column_to_str(column_data_frame):
    return column_data_frame.apply(
        lambda value:
        str(value).strip()
    )


def convert_column_datetime_to_date(column_data_frame):
    return column_data_frame.apply(
        lambda datetime:
        str(datetime).split(" ")[0]
    )


def convert_column_to_date(column_data_frame,
                           format_,
                           default):
    def if_1(date):
        return len(str(date).replace('/', '').replace('-', '').strip()) == 8

    return pd.to_datetime(
        arg=column_data_frame.apply(
            lambda date:
            str(date).replace('/', '').replace('-', '').strip()
            if if_1(date) else
            default
        ),
        format=format_
    )


def convert_column_to_tittle(column_data_frame):
    return column_data_frame.apply(
        lambda value:
        str(value).strip().title()
    )


def convert_column_to_upper(column_data_frame):
    return column_data_frame.apply(
        lambda value:
        str(value).strip().upper()
    )


def convert_column_cpf_to_int64(column_data_frame,
                                default):
    def if_1(cpf):
        str_ = str(cpf).replace("-", "").replace(".", "").replace(" ", "").strip()

        return (
            str_.isnumeric()
            and
            len(str_) == 11
        )

    return column_data_frame.apply(
        lambda cpf:
        int(str(cpf).replace("-", "").replace(".", "").replace(" ", "").strip())
        if if_1(cpf) else
        int(default)
    )


def convert_int_cpf_to_format_cpf(column_data_frame):
    return column_data_frame.apply(
        lambda cpf:
        f"{int(str(f'{int(cpf):011}')[:3]):03}"
        f".{int(str(f'{int(cpf):011}')[3:6]):03}"
        f".{int(str(f'{int(cpf):011}')[6:9]):03}"
        f"-{int(str(f'{int(cpf):011}')[9:]):02}"
    )


def convert_column_cnpj_to_int64(column_data_frame,
                                 default):
    def if_1(cnpj):
        str_ = str(cnpj).replace("-", "").replace(".", "").replace("/", "").replace(" ", "").strip()

        return (
            str_.isnumeric()
            and
            len(str_) == 14
        )

    return column_data_frame.apply(
        lambda cnpj:
        int(str(cnpj).replace("-", "").replace(".", "").replace("/", "").replace(" ", "").strip())
        if if_1(cnpj) else
        int(default)
    )


def convert_int_cnpj_to_format_cnpj(column_data_frame):
    return column_data_frame.apply(
        lambda cnpj:
        f"{int(str(f'{int(cnpj):011}')[:2]):02}"
        f".{int(str(f'{int(cnpj):011}')[2:5]):03}"
        f".{int(str(f'{int(cnpj):011}')[5:8]):03}"
        f"/{int(str(f'{int(cnpj):011}')[8:12]):04}"
        f"-{int(str(f'{int(cnpj):011}')[12:]):02}"
    )


def convert_column_datetime_to_hour(column_data_frame,
                                    default):
    def if_1(date):
        return str(date).split(" ")[1].split(":")[0].isnumeric()

    return column_data_frame.apply(
        lambda date:
        int(str(date).split(" ")[1].split(":")[0])
        if if_1(date) else
        int(default)
    )


def convert_column_datetime_to_day(column_data_frame,
                                   default):
    def if_1(date):
        return str(date).split(" ")[0].split("-")[2].isnumeric()

    return column_data_frame.apply(
        lambda date:
        int(str(date).split(" ")[0].split("-")[2])
        if if_1(date) else
        int(default)
    )


def convert_column_datetime_to_month(column_data_frame,
                                     default):
    def if_1(date):
        return str(date).split(" ")[0].split("-")[1].isnumeric()

    return column_data_frame.apply(
        lambda date:
        int(str(date).split(" ")[0].split("-")[1])
        if if_1(date) else
        int(default)
    )


def convert_column_datetime_to_year(column_data_frame,
                                    default):
    def if_1(date):
        return str(date).split(" ")[0].split("-")[0].isnumeric()

    return column_data_frame.apply(
        lambda date:
        int(str(date).split(" ")[0].split("-")[0])
        if if_1(date) else
        int(default)
    )


def insert_row(df,
               row,
               values):
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


def insert_default_values_table(df,
                                reset_index=False):
    k_numerics = df.select_dtypes(
        include="number"
    ).keys().to_list()

    k_datetime = df.select_dtypes(
        include="datetime"
    ).keys().to_list()

    df_default = pd.DataFrame(
        data={
            k: [-3, -2, -1]
            if k in k_numerics
            else [dt.datetime(1900, 1, 1) for _ in range(3)]
            if k in k_datetime
            else ["Desconhecido", "Não Aplicável", "Não Informado"]
            for k in df.keys()
        }
    )

    df_final = pd.concat([
        df_default,
        df
    ])

    if reset_index:
        return df_final.reset_index(
            drop=True
        )

    return df_final


def multiply_columns(frame,
                     col_1,
                     col_2):
    return frame[col_1] * frame[col_2]


def create_sk_categoria(column):
    categorias = {
        1: [
            "CAFE", "ACHOCOLATADO", "CEREAL", "CEREAIS", "PAO",
            "ACUCAR", "ADOCANTE", "BISCOITO", "GELEIA", "IORGUTE"
        ],
        2: [
            "ARROZ", "FEIJAO", "FARINHA DE TRIGO", "AMIDO DE MILHO", "FERMENTO", "MACARRAO",
            "MOLHO DE TOMATE", "AZEITE", "OLEO", "OVOS", "TEMPEROS", "SAL", "SAZON",
            "FARINHA DE AVEIA", "FANDANGOS"
        ],
        3: [
            "BIFE", "FRANGO", "PEIXE", "CARNE MOIDA", "SALSICHA", "LINGUICA"
        ],
        4: [
            "SUCO", "CERVEJA", "REFRIGERANTE", "VINHO"
        ],
        5: [
            "SABONETE", "CREME DENTAL", "SHAMPOO", "CONDICIONADOR", "ABSORVENTE", "PAPEL HIGIENICO",
            "FRALDA"
        ],
        6: [
            "LEITE", "PRESUNTO", "QUEIJO", "REQUEIJAO", "MANTEIGA", "CREME DE LEITE"
        ],
        7: [
            "AGUA SANITARIA", "SABAO EM PO", "PALHA DE ACO", "AMACIANTE", "DETERGENTE",
            "SACO DE LIXO", "DESINFETANTE", "PAPEL TOALHA"
        ],
        8: [
            "ALFACE", "CEBOLA", "ALHO", "TOMATE", "LIMAO", "BANANA", "MACA", "BATATA"
        ]
    }

    def categorized(name):
        name_ud = ud.unidecode(name.upper())

        for k, v in categorias.items():
            if name_ud in v:
                return k

            else:
                for n in name_ud.split():
                    for cat in v:
                        if n in cat:
                            return k

        return -3

    return column.apply(
        lambda name:
        categorized(name)
    )


def create_sk_turno(column):
    def categorized(hour):
        turnos = {
            1: range(6, 12),
            2: range(12, 18),
            3: range(18, 24),
            4: range(0, 6),
        }

        for k, v in turnos.items():
            if hour in v:
                return k

        return -3

    return column.apply(
        lambda hour:
        categorized(hour)
    )


def create_table(conn_output,
                 schema_name,
                 table_name,
                 table_vars):
    str_vars = ""

    for k, v in table_vars.items():
        str_vars += f"{k} {v}, "

    str_vars = str_vars[:-2]

    conn_output.execute(f"create table if not exists \"{schema_name.lower()}\".\"{table_name}\" ({str_vars})")


def create_schema(database,
                  schema_name):
    database.execute(f" create schema if not exists {schema_name.lower()}")


def drop_tables(conn_output,
                schema_name,
                dimensions_names):
    for i in range(len(dimensions_names)):
        conn_output.execute(
            f" drop table if exists \"{schema_name.lower()}\".\"{dimensions_names[i]}\""
        )


def drop_schemas(conn_output,
                 schemas_names):
    for schema in schemas_names:
        conn_output.execute(f" drop schema if exists {schema.lower()}")


def delete_register_from_table(conn_output,
                               schema_name,
                               table_name,
                               where):
    conn_output.execute(
        f"delete from \"{schema_name}\".\"{table_name}\" where {where}"
    )


def update_register_from_table(conn_output,
                               schema_name,
                               table_name,
                               set_,
                               where):
    conn_output.execute(
        f"update \"{schema_name}\".\"{table_name}\" set {set_} where {where}"
    )


def set_ativo(row):
    row.ativo_x = 0
    row.ativo_y = 0

    if row.data_cadastro_x > row.data_cadastro_y:
        row.ativo_x = 1

    else:
        row.ativo_y = 1

    return row


def compare_two_columns(column_1,
                        column_2):
    return column_1 == column_2


def generate_date_table(start_date,
                        end_date,
                        frequency="D"):
    """
    frequency values
    https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timeseries-offset-aliases

    :param start_date:
        Data de início da contagem, de preferência no formato USA "2021-05-01"
    :param end_date:
        Data de fim da contagem, de preferência no formato USA "2021-01-01"
    :param frequency:
        Frequência em que serão contadas as datas, "D" = dias, "M" = mêses, "H" = horas, "min" = minutos, etc.
    :return:
        Retorna um dataframe com as datas geradas no período especificado
    """

    order_columns = [
        "sk_data",
        "dt_referencia",
        "dt_semestre",
        "dt_trimestre",
        "dt_bimestre",
        "dt_ano",
        "dt_mes",
        "dt_dia",
        "dt_hora",
        "dt_minuto",
        "dt_segundo"
    ]

    return pd.DataFrame({
        "dt_referencia": pd.date_range(
            start=start_date,
            end=end_date,
            freq=frequency
        )
    }).assign(
        sk_data=lambda df: create_index_dataframe(df, 1),
        dt_ano=lambda df: df.dt_referencia.apply(
            lambda value: value.year
        ),
        dt_mes=lambda df: df.dt_referencia.apply(
            lambda value: value.month
        ),
        dt_dia=lambda df: df.dt_referencia.apply(
            lambda value: value.day
        ),
        dt_hora=lambda df: df.dt_referencia.apply(
            lambda value: value.hour
        ),
        dt_minuto=lambda df: df.dt_referencia.apply(
            lambda value: value.minute
        ),
        dt_segundo=lambda df: df.dt_referencia.apply(
            lambda value: value.second
        ),
        dt_semestre=lambda df: df.dt_mes.apply(
            func=lambda value: (
                1
                if 1 <= value <= 6
                else 2
            )
        ),
        dt_trimestre=lambda df: df.dt_mes.apply(
            func=lambda value: (
                1
                if 1 <= value <= 3
                else 2
                if 4 <= value <= 6
                else 3
                if 7 <= value <= 9
                else 4
            )
        ),
        dt_bimestre=lambda df: df.dt_mes.apply(
            func=lambda value: (
                1
                if 1 <= value <= 2
                else 2
                if 3 <= value <= 4
                else 3
                if 5 <= value <= 6
                else 4
                if 7 <= value <= 8
                else 5
                if 9 <= value <= 10
                else 6
            )
        )
    ).filter(order_columns)
