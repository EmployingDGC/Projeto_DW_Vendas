import pandas as pd
import DW_TOOLS as dwt
import connection as con
from sqlalchemy.types import Integer, String
from pandasql import sqldf
import sqlalchemy as sqla

def extract_dim_cliente(connection):

    tbl_cliente = dwt.read_table(conn=connection, schema="stage", table_name="stg_cliente",
                                 columns=["id_cliente", "nome", "cpf", "tel",
                                          "id_endereco"])

    stg_endereco = dwt.read_table(conn=connection, schema="stage", table_name="stg_endereco",
                                  columns=["id_endereco", "estado", "cidade", "bairro",
                                           "rua"])

    stg_cliente = (
        tbl_cliente.merge(right=stg_endereco, left_on="id_endereco", right_on="id_endereco",
                          how="left", suffixes=["_01", "_02"])
    )

    if sqla.inspect(connection).has_table(table_name='d_cliente', schema='dw'):
        query = """
            SELECT stg.*
            FROM stage.stg_cliente stg
            LEFT JOIN dw.d_cliente cliente 
            ON (stg.id_cliente = cliente.cd_cliente)
            WHERE cliente.cd_cliente IS NULL
        """
        stg_cliente = sqldf(query, {"stg_clinte": stg_cliente}, connection.url)

    return stg_cliente

def treat_dim_cliente(connection, tbl, exists_dim):
    columns_select = ['id_cliente', 'nome', 'cpf', 'tel', 'id_endereco', 'estado',
                      'cidade', 'bairro', 'rua']


    columns_name = {
        "id_cliente": "cd_cliente",
        "nome": "no_cliente",
        "cpf": "nu_cpf",
        "tel": "nu_telefone",
        "id_endereco": "cd_endereco",
        "estado": "no_estado",
        "cidade": "no_cidade",
        "bairro": "no_bairro",
        "rua": "no_rua"
    }

    dim_cliente = (
        tbl.
            filter(columns_select).
            rename(columns=columns_name)
    )

    if exists_dim:

        sk_index = dwt.find_sk(conn=connection, schema_name='dw', table_name='d_cliente',
                               sk_name='sk_cliente')

        dim_cliente.insert(0, "sk_cliente", range(sk_index, sk_index + len(dim_cliente)))

    else:
        dim_cliente.insert(0, "sk_cliente", range(1, 1 + len(dim_cliente)))


        dim_cliente = (
            pd.DataFrame([
                [-1, -1, "Não Informado", "Não Informado", "Não Informado", -1,
                 "Não Informado", "Não Informado", "Não Informado", "Não Informado"],
                [-2, -2, "Não Aplicável", "Não Aplicável", "Não Aplicável", -2,
                 "Não Aplicável", "Não Aplicável", "Não Aplicável", "Não Aplicável"],
                [-3, -3, "Desconhecido", "Desconhecido", "Desconhecido", -3,
                 "Desconhecido", "Desconhecido", "Desconhecido", "Desconhecido"]
            ], columns=dim_cliente.columns).append(dim_cliente)
        )

    return dim_cliente

def load_dim_cliente(d_cliente, connection):

    data_types = {
        'sk_cliente': Integer(), 'cd_cliente': Integer(), 'no_cliente': String(),
        'nu_cpf': String(), 'nu_telefone': String(), 'cd_endereco': Integer(),
        'no_estado': String(), 'no_cidade': String(), 'no_bairro': String(),
        'no_rua': String()
    }

    (
        d_cliente.
            astype('string').
            to_sql(name="d_cliente", con=connection, schema="dw", if_exists="append",
                   dtype=data_types, index=False)
    )

def run_dim_cliente(connection):

    # testa 2x
    fl_dim = sqla.inspect(connection).has_table(table_name='d_cliente', schema='dw')

    tbl_cliente = extract_dim_cliente(connection=connection)

    # não trata o update?
    if tbl_cliente.shape[0] != 0:
        (
            treat_dim_cliente(connection=connection, tbl=tbl_cliente, exists_dim=fl_dim).
                pipe(load_dim_cliente, con_out=connection)
        )

if __name__ == '__main__':

    conexao_postgre = con.create_connection_postgre("127.0.0.1,", "ProjetoDW_Vendas",
                                                    "airflow", "666itix", 5432)
    run_dim_cliente(conexao_postgre)
