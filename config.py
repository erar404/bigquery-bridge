import os

mssql_server = os.getenv("MSSQL_SERVER", "localhost")
mssql_database = os.getenv("MSSQL_DATABASE", "default_db")
mssql_user = os.getenv("MSSQL_USER", "sa")
mssql_password = os.getenv("MSSQL_PASSWORD", "sqlserver")
mssql_driver = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")
mssql_instance = os.getenv("MSSQL_INSTANCE", "") # project:region:instance-name

bigquery_project_id = os.getenv("BIGQUERY_PROJECT_ID", "default_project")
bigquery_dataset_id = os.getenv("BIGQUERY_DATASET_ID", "default_dataset")
