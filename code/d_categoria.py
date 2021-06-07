import pandas as pd

import utilities as utl


def get():
    all_categories = [
        "Café da Manhã",
        "Mercearia",
        "Carnes",
        "Bebidas",
        "Higiene",
        "Laticínios / Frios",
        "Limpeza",
        "Hortifruti"
    ]

    return pd.DataFrame(
        data={
            "SK_CATEGORIA": [i + 1 for i in range(len(all_categories))],
            "DS_CATEGORIA": [c for c in all_categories]
        }
    )


# def treat() -> pd.DataFrame:
#     pass


def run(conn_input):
    utl.create_schema(conn_input, "dw")

    get().to_sql(
        name="D_CATEGORIA",
        con=conn_input,
        schema="dw",
        if_exists="replace",
        index=False,
        chunksize=10000
    )


# cafe_manha = (
#     "CAFÉ",
#     "ACHOCOLATADO",
#     "CEREAIS",
#     "PÃO",
#     "AÇÚCAR",
#     "SUCO",
#     "ADOÇANTE",
#     "BISCOITOS",
#     "GELÉIA",
#     "IORGUTE"
# )
#
# mercearia = (
#     "ARROZ",
#     "FEIJÃO",
#     "FARINHA DE TRIGO",
#     "AMIDO DE MILHO",
#     "FERMENTO",
#     "MACARRÃO",
#     "MOLHO DE TOMATE",
#     "AZEITE",
#     "ÓLEO",
#     "OVOS",
#     "TEMPEROS",
#     "SAL"
# )
#
# carnes = (
#     "BIFE BOVINO",
#     "FRANGO",
#     "PEIXE",
#     "CARNE MOÍDA",
#     "SALSICHA/LINGUICA"
# )
#
# bebidas = (
#     "SUCOS",
#     "CERVEJAS",
#     "REFRIGERANTES"
# )
# higiene = (
#     "SABONETE",
#     "CREME DENTAL",
#     "SHAMPOO/CONDICIONADOR",
#     "ABSORVENTE",
#     "PAPEL HIGIÊNICO"
# )
#
# laticinios_frios = (
#     "LEITE",
#     "PRESUNTO/QUEIJO",
#     "REQUIJÃO",
#     "MANTEIGA",
#     "CREME DE LEITE"
# )
#
# limpeza = (
#     "ÁGUA SANITÁRIA",
#     "SABÃO EM PÓ",
#     "PALHA DE AÇO",
#     "AMACIANTE",
#     "DETERGENTE",
#     "SACO DE LIXO",
#     "DESINFETANTE",
#     "PAPEL TOALHA"
# )
# hortfruti = (
#     "ALFACE",
#     "CEBOLA",
#     "ALHO",
#     "TOMATE",
#     "LIMÃO",
#     "BANANA",
#     "MAÇÃ",
#     "BATATA"
# )
