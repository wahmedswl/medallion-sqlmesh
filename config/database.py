from pydantic import BaseModel
import os


class Config(BaseModel):
    host: str
    port: int
    database: str
    username: str
    password: str


def sql_server_config() -> Config:
    HOST = os.getenv("SQLMESH_SQL_SERVER_HOST")
    PORT = os.getenv("SQLMESH_SQL_SERVER_PORT")
    DATABASE = os.getenv("SQLMESH_SQL_SERVER_DATABASE")
    USERNAME = os.getenv("SQLMESH_SQL_SERVER_USERNAME")
    PASSWORD = os.getenv("SQLMESH_SQL_SERVER_PASSWORD")

    return Config(
        host=HOST, port=PORT, database=DATABASE, username=USERNAME, password=PASSWORD
    )
