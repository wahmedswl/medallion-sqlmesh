import typing as t
import os
from datetime import datetime
import pandas as pd
import pyodbc
import logging
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName

from config.database import sql_server_config


@model(
    "medallion.bl_users",
    kind=dict(name=ModelKindName.FULL),
    columns={
        "Email": "str",
        "Active": "int",
        "FilterType": "str",
        "IsExternal": "int",
        "CreatedAt": "datetime",
        "inserted_at": "datetime",
        "inserted_by": "str",
    },
    cron="@daily",
    start="2020-01-01",
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    config = sql_server_config()
    driver = "ODBC Driver 17 for SQL Server"
    conn_str = f"DRIVER={{{driver}}};SERVER={config.host};DATABASE={config.database};UID={config.username};PWD={config.password};"

    try:
        conn = pyodbc.connect(conn_str)
        logging.info("connected with the database ...")

        source_table = "PowerBIUsers"
        sql_query = f"SELECT * FROM {source_table};"
        params = [start, end]
        # Fetch data into DataFrame
        df = pd.read_sql_query(sql_query, conn)

        df["inserted_at"] = pd.Timestamp("now")
        df["inserted_by"] = "sqlmesh.pandas-external_users"

        return df
    except pyodbc.Error as e:
        logging.error(f"Database error: {e}")
    except Exception as ex:
        logging.error(f"An error occurred: {ex}")
    finally:
        conn.close()
        logging.info("Database connection closed.")
