class CafeManha:
    value = "Café da Manhã"
    keys = (
        "CAFÉ",
        "ACHOCOLATADO",
        "CEREAIS",
        "PÃO",
        "AÇÚCAR",
        "SUCO",
        "ADOÇANTE",
        "BISCOITOS",
        "GELÉIA",
        "IORGUTE"
    )


class Mercearia:
    value = "Mercearia"
    keys = (
        "ARROZ",
        "FEIJÃO",
        "FARINHA DE TRIGO",
        "AMIDO DE MILHO",
        "FERMENTO",
        "MACARRÃO",
        "MOLHO DE TOMATE",
        "AZEITE",
        "ÓLEO",
        "OVOS",
        "TEMPEROS",
        "SAL"
    )


class Carnes:
    value = "Carnes"
    keys = (
        "BIFE BOVINO",
        "FRANGO",
        "PEIXE",
        "CARNE MOÍDA",
        "SALSICHA/LINGUICA"
    )


class Bebidas:
    value = "Bebidas"
    keys = (
        "SUCOS",
        "CERVEJAS",
        "REFRIGERANTES"
    )


class Higiene:
    value = "Higiene"
    keys = (
        "SABONETE",
        "CREME DENTAL",
        "SHAMPOO/CONDICIONADOR",
        "ABSORVENTE",
        "PAPEL HIGIÊNICO"
    )


class LaticiniosFrios:
    value = "Laticínios / Frios"
    keys = (
        "LEITE",
        "PRESUNTO/QUEIJO",
        "REQUIJÃO",
        "MANTEIGA",
        "CREME DE LEITE"
    )


class Limpeza:
    value = "Limpeza"
    keys = (
        "ÁGUA SANITÁRIA",
        "SABÃO EM PÓ",
        "PALHA DE AÇO",
        "AMACIANTE",
        "DETERGENTE",
        "SACO DE LIXO",
        "DESINFETANTE",
        "PAPEL TOALHA"
    )


class Hotifruti:
    value = "Hortifruti"
    keys = (
        "ALFACE",
        "CEBOLA",
        "ALHO",
        "TOMATE",
        "LIMÃO",
        "BANANA",
        "MAÇÃ",
        "BATATA"
    )


uncategorized = "Não Categorizado"

all_categories = [
    CafeManha.value,
    Mercearia.value,
    Carnes.value,
    Bebidas.value,
    Higiene.value,
    LaticiniosFrios.value,
    Limpeza.value,
    Hotifruti.value
]

sk = "SK_CATEGORIA"
ds = "DS_CATEGORIA"
