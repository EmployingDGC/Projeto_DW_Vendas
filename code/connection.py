from sqlalchemy.engine.mock import MockConnection

import sqlalchemy as sa


def create_connection_postgre(server: str,
                              database: str,
                              username: str,
                              password: str,
                              port: int) -> MockConnection:
    conn = f"postgresql+psycopg2://{username}:{password}@{server}:{port}/{database}"
    return sa.create_engine(conn)
