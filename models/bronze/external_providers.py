import typing as t
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName


@model(
    "medallion.bl_providers",
    kind=dict(name=ModelKindName.FULL),
    columns={
        "FullName": "str",
        "Active": "int",
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
    server = "monitoring-srvr.tail163fe.ts.net"
    database = "ReportsSavi"
    username = "sa"
    password = "M100HMS$ql@2023"

    spark = (
        SparkSession.builder.appName("medallion-sqlmesh")
        .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:11.2.0.jre8")
        .getOrCreate()
    )

    jdbc_url = f"jdbc:sqlserver://{server}:1433;databaseName={database};encrypt=true;trustServerCertificate=true;"
    table_name = "dbo.PowerBIProviders"

    connection_properties = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }

    df = spark.read.jdbc(
        url=jdbc_url, table=table_name, properties=connection_properties
    )

    df = df.withColumn("inserted_at", current_timestamp())
    df = df.withColumn("inserted_by", lit("sqlmesh.spark-external_providers"))

    return df.toPandas()
