class Configuration:
    class Connection:
        server = "10.0.0.102"
        database = "projeto_dw_vendas"
        username = "postgres"
        password = "itix.123"
        port = 5432

    rows_per_data_frame = 10000


class Schema:
    client = "public"
    stage = "stage"
    dw = "dw"


class TablesClient:
    cliente = "CLIENTE"
    endereco = "ENDERECO"
    forma_pagamento = "FORMA_PAGAMENTO"
    funcionario = "FUNCIONARIO"
    item_venda = "ITEM_VENDA"
    loja = "LOJA"
    produto = "PRODUTO"
    venda = "VENDA"


class TablesSTG:
    cliente = f"STG_{TablesClient.cliente}"
    endereco = f"STG_{TablesClient.endereco}"
    forma_pagamento = f"STG_{TablesClient.forma_pagamento}"
    funcionario = f"STG_{TablesClient.funcionario}"
    item_venda = f"STG_{TablesClient.item_venda}"
    loja = f"STG_{TablesClient.loja}"
    produto = f"STG_{TablesClient.produto}"
    venda = f"STG_{TablesClient.venda}"


class TablesDW:
    categoria = "D_CATEGORIA"


class Dimension:
    class Categoria:
        sk = "SK_CATEGORIA"
        ds = "DS_CATEGORIA"


class Category:
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
