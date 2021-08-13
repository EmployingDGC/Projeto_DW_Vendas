import pandas as pd
import CONEXAO as con
import DW_TOOLS as dwt
from sqlalchemy.types import Integer, String, DateTime, Float, Date
from datetime import datetime
from pandasql import sqldf

def extract_fact_venda(con_out, date):

    stg_venda= dwt.read_table(conn=con_out, schema="stage", table_name="stg_venda",
                              columns=['id_venda', 'id_pagamento', 'id_cliente',
                                       'id_func', 'id_loja', 'data_venda', 'nfc'],
                              where=f"data_venda > '{date}'")

    if stg_venda.shape[0] != 0:

        d_cliente = dwt.read_table(conn=con_out, schema="dw", table_name="d_cliente",
                                   columns=['sk_cliente', 'cd_cliente'])

        d_funcionario = dwt.read_table(conn=con_out, schema="dw", table_name="d_funcionario",
                                       columns=['sk_funcionario', 'cd_funcionario'])

        d_forma_pg = dwt.read_table(conn=con_out, schema="dw", table_name="d_forma_pagamento",
                                    columns=['sk_forma_pagamento', 'cd_forma_pagamento'])

        d_calendar = dwt.read_table(conn=con_out, schema="dw", table_name="d_data",
                                    columns=['sk_data', 'dt_referencia'])

        stg_item_venda = dwt.read_table(conn=con_out, schema="stage",
                                        table_name="stg_item_venda")


        tbl_venda_temp = (
            stg_venda.assign(
                data_venda_2=lambda x: x.data_venda.astype('datetime64').dt.floor('h')
            ).
                merge(right=stg_item_venda, how="inner", on="id_venda",
                      suffixes=["_01", "_02"]).

                pipe(dwt.merge_input, right=d_cliente, left_on="id_cliente",
                     right_on="cd_cliente", suff=["_03", "_04"],
                     surrogate_key="sk_cliente").

                pipe(dwt.merge_input, right=d_funcionario, left_on="id_func",
                     right_on="cd_funcionario", suff=["_05", "_06"],
                     surrogate_key="sk_funcionario").

                pipe(dwt.merge_input, right=d_forma_pg, left_on="id_pagamento",
                     right_on="cd_forma_pagamento", suff=["_07", "_08"],
                     surrogate_key="sk_forma_pagamento").

                pipe(dwt.merge_input, right=d_calendar, left_on="data_venda_2",
                     right_on="dt_referencia", suff=["_09", "_10"],
                     surrogate_key="sk_data")
        )

        query = """
                    SELECT t.*, p.sk_produto, p.vl_lucro, p.vl_preco, l.sk_loja
                     FROM venda t
                     LEFT JOIN dw.d_produto p
                     ON (t.id_produto = p.cd_produto
                            AND date(t.data_venda) >= date(p.dt_vigencia_inicio)
                            AND (date(t.data_venda) < date(p.dt_vigencia_fim) 
                                OR p.dt_vigencia_fim IS NULL)
                     )
                     LEFT JOIN dw.d_loja l
                     ON (t.id_loja = l.cd_loja
                            AND date(t.data_venda) >= date(l.dt_vigencia_inicio) 
                            AND (date(t.data_venda) < date(l.dt_vigencia_fim)
                                OR l.dt_vigencia_fim IS NULL)
                    )
                """

        tbl_venda = sqldf(
            query, {"venda": tbl_venda_temp}
            , con_out.url
        )
    else:
        tbl_venda=stg_venda

    return tbl_venda

def treat_fact_venda(tbl):
    columns_select = ['sk_data', 'sk_loja', 'sk_cliente', 'sk_forma_pagamento',
                      'sk_produto', 'qtd_produto', 'vl_preco', 'vl_lucro', 'cd_venda',
                      'nu_nfc', 'dt_venda', 'dt_carga']

    columns_names = {
        'id_venda': 'cd_venda',
        'nfc': 'nu_nfc',
        'data_venda': 'dt_venda',
        'qtd_produto': 'qtd_produto'
    }

    fact_venda = (
        tbl.
            rename(columns=columns_names).
            filter(columns_select).
            astype({"dt_venda": "datetime64[ns]"}).
            assign(
            sk_produto=lambda x: x["sk_produto"].fillna(value=-3).astype("Int64"),
            sk_loja=lambda x: x["sk_loja"].fillna(value=-3).astype("Int64"),
            dt_carga=pd.to_datetime(datetime.today(), format='%Y-%m-%d')
        )
    )

    return fact_venda

def load_fact_venda(f_venda, conn_out):

    data_type = {'sk_data': Integer(), 'sk_loja': Integer(), 'sk_cliente': Integer(),
                 'sk_forma_pagamento': Integer(), 'sk_produto': Integer(),
                 'qtd_produto': Integer(), 'cd_venda': Integer(), 'vl_preco': Float(),
                 'vl_lucro': Float(), 'nu_nfc': String(), 'dt_venda': DateTime(),
                 'dt_carga': Date()
                 }
    (
        f_venda.
            astype('string').
            to_sql(name="f_venda_item", con=conn_out, schema="dw", if_exists="append",
                   index=False, dtype=data_type)
    )

def run_fact_venda(con_out):

    result = con_out.execute('SELECT MAX("dt_venda") FROM dw."f_venda_item"')
    date_max = result.fetchone()[0]

    load_date = datetime.strftime(date_max, "%Y-%m-%d %H:%M:%S")

    tbl_fact = extract_fact_venda(con_out, load_date)

    if tbl_fact.shape[0] != 0:
        (
            treat_fact_venda(tbl=tbl_fact).
                pipe(load_fact_venda, conn_out=con_out)
        )



if __name__ == '__main__':
    conexao_postgre = con.connect_postgre("127.0.0.1,", "ProjetoDW_Vendas",
                                          "airflow", "666itix", 5432)

    run_fact_venda(con_out=conexao_postgre)