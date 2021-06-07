class Connection:
    server = "10.0.0.102"
    database = "projeto_dw_vendas"
    username = "postgres"
    password = "itix.123"
    port = 5432


class ToSQL:
    class IfExists:
        replace = "replace"
        append = "append"

    class Index:
        yes = True
        no = False

    rows_per_data_frame = 10000
