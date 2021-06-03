class Connection:
    server = "10.0.0.102"
    database = "projeto_dw_vendas"
    username = "postgres"
    password = "itix.123"
    port = 5432


class Schemas:
    client = "public"
    stage = "stage"
    dw = "dw"


class TablesClient:
    cliente = "CLIENTE"
    endereco = "ENDERECO"
    forma_pagamento = "FORMA_PAGAMENTO"
    funcionario = "FUCIONARIO"  # no banco está escrito errado, está faltando o N
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
