from datetime import datetime, date
import pandas as pd
from pandasql import sqldf
import CONEXAO as con
import DW_TOOLS as dwt
from sqlalchemy.types import Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
import sqlalchemy as sqla

def update_dim_loja(dim_update, conn):

    Session = sessionmaker(bind=conn)
    session = Session()
    metadata = sqla.MetaData(bind=conn)

    datatable = sqla.Table('d_loja', metadata, schema='dw', autoload=True)

    update = (
        sqla.sql.update(datatable).values(
            {'fl_ativo': 0, 'dt_vigencia_fim': datetime.today()}
        ).
            where(
            sqla.and_(
                datatable.c.cd_loja.in_(dim_update.cd_loja), datatable.c.fl_ativo == 1
            )
        )
    )

    session.execute(update)
    session.flush()
    session.commit()

def extract_dim_loja(con_out, exists_dim):

    stg_loja = dwt.read_table(conn=con_out, schema="stage", table_name="stg_loja",
                         columns=["id_loja", "nome_loja", "razao_social", "cnpj",
                                  "id_endereco"])

    stg_endereco = dwt.read_table(conn=con_out, schema="stage", table_name="stg_endereco",
                                  columns=["id_endereco", "estado", "cidade", "bairro",
                                           "rua"])

    tbl_loja = (
        stg_loja.merge(right=stg_endereco, left_on="id_endereco", right_on="id_endereco",
                       how="left", suffixes=["_01", "_02"])
    )

    if exists_dim:

        query = """
        SELECT tbl.*, dim.dt_vigencia_fim,
                CASE
                    WHEN dim.cd_loja IS NULL 
                    THEN 'I'
                    WHEN dim.cd_endereco        != tbl.id_endereco
                        OR dim.ds_razao_social  != tbl.razao_social
                        OR dim.nu_cnpj          != tbl.cnpj
                    THEN 'U'
                    ELSE 'N'
                END as fl_insert_update
                FROM tbl_loja tbl
                LEFT JOIN  dw.d_loja dim 
                ON (tbl.id_loja = dim.cd_loja)
                AND dim.fl_ativo = 1
        """

        tbl_loja = sqldf(query, {"tbl_loja": tbl_loja}, con_out.url
        )

    else:
        tbl_loja = (
            tbl_loja.assign(
                fl_insert_update='I'
            )
        )

    return tbl_loja


def treat_dim_loja(tbl, con_out, exists_dim):

    columns_select = ["cd_loja", "no_loja", "ds_razao_social", "nu_cnpj", "cd_endereco",
                      "no_estado", "no_cidade", "no_bairro", "no_rua", 'fl_ativo',
                      'dt_vigencia_inicio', 'dt_vigencia_fim', 'fl_insert_update']

    columns_name = {
        "id_loja": "cd_loja", "nome_loja": "no_loja", "razao_social": "ds_razao_social",
        "cnpj": "nu_cnpj", "id_endereco": "cd_endereco", "estado": "no_estado",
        "cidade": "no_cidade", "bairro": "no_bairro", "rua": "no_rua"
    }

    d_loja = (
        tbl.
            rename(columns=columns_name).
            filter(columns_select).
            assign(
            dt_vigencia_inicio=datetime.today(),
            fl_ativo=1
        )
    )

    if exists_dim:

        sk_index = dwt.find_sk(conn=con_out, schema_name='dw', table_name='d_loja',
                              sk_name='sk_loja')

        d_loja.insert(0, "sk_loja", range(sk_index, sk_index + len(d_loja)))


    else:

        columns_select = ["sk_loja", "cd_loja", "no_loja", "ds_razao_social", "nu_cnpj",
                          "cd_endereco", "no_estado", "no_cidade", "no_bairro", "no_rua",
                          'dt_vigencia_inicio', 'dt_vigencia_fim', 'fl_ativo']


        d_loja.insert(0, "sk_loja", range(1, 1 + len(d_loja)))

        default_data = pd.to_datetime("1900-01-01", format='%Y-%m-%d')

        d_loja = (
            d_loja.assign(
                dt_vigencia_fim=None
            ).
                drop("fl_insert_update", axis=1).
                filter(columns_select)
        )

        d_loja = (
            pd.DataFrame([
                [-1, -1, "Não Informado", "Não Informado", "Não Informado", -1,
                 "Não Informado", "Não Informado", "Não Informado", "Não Informado", default_data,
                 None, -1],
                [-2, -2, "Não Aplicável", "Não Aplicável", "Não Aplicável", -2,
                 "Não Aplicável", "Não Aplicável", "Não Aplicável", "Não Aplicável", default_data,
                 None, -2],
                [-3, -3, "Desconhecido", "Desconhecido", "Desconhecido", -3,
                 "Desconhecido", "Desconhecido", "Desconhecido", "Desconhecido", default_data,
                 None, -3],
            ], columns=d_loja.columns).append(d_loja)
        )

    return d_loja


def load_dim_loja(dim_loja, con_out, exists_dim):

    data_types = {
        'sk_loja': Integer(), 'cd_loja': Integer(), 'no_loja': String(),
        'ds_razao_social': String(), 'nu_cnpj': String(), 'cd_endereco': Integer(),
        'no_estado': String(), 'no_cidade': String(), 'no_bairro': String(),
        'no_rua': String(), 'dt_vigencia_inicio': DateTime(),
        'dt_vigencia_fim': DateTime(), 'fl_ativo': Integer()
    }

    if exists_dim:

        df_update = dim_loja.query("fl_insert_update == 'U'")
        del dim_loja['fl_insert_update']

        if df_update.shape[0] > 0:
            update_dim_loja(df_update, con_out)

    (
        dim_loja.astype('string').
            to_sql(
            name="d_loja", con=con_out, schema="dw", if_exists="append", index=False,
            dtype=data_types
        )
    )

def run_dim_loja(conn):

    fl_dim = sqla.inspect(conn).has_table(table_name='d_loja', schema='dw')

    tbl_loja = extract_dim_loja(con_out=conn, exists_dim=fl_dim)

    df_loja = tbl_loja.query("fl_insert_update == 'I' or fl_insert_update == 'U'")


    if df_loja.shape[0] > 0:
        (
            treat_dim_loja(tbl=tbl_loja, con_out=conn, exists_dim=fl_dim).
                pipe(load_dim_loja, con_out=conn, exists_dim=fl_dim)
        )


if __name__ == '__main__':
    conn_out = con.connect_postgre("127.0.0.1,", "ProjetoDW_Vendas",
                                    "airflow", "666itix", 5432)

    run_dim_loja(conn=conn_out)
