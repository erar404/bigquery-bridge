import os
import pandas_gbq
import pyodbc
import config
import sqlalchemy
import pytds # The SQL Server driver
from urllib.parse import quote_plus
from google.cloud.sql.connector import Connector

class DbConn(object):
    """Class to manage database connection strings for BigQuery and MSSQL."""

    def __init__(self, logger):
        """Class Init."""
        self.__bigquery_conn_str = self.get_bigquery_connection_string()
        self.__logger = logger
        self.__connector = self.get_mssql_connector()
        self.__mssql_engine = self.get_mssql_engine()
        
        # self.test_connections()

    def get_bigquery_connection_string(self):
        """
        Constructs and returns the BigQuery connection string using environment variables.
        """
        project_id = os.getenv("BIGQUERY_PROJECT_ID", "default_project")
        dataset_id = os.getenv("BIGQUERY_DATASET_ID", "default_dataset")
        
        connection_string = f"bigquery://{project_id}/{dataset_id}"
        return connection_string
    
    def get_mssql_connector(self):
        """Creates and returns a Cloud SQL Connector for MSSQL."""
        connector = Connector(
            ip_type="public",  # can also be "private" or "psc"
            enable_iam_auth=False,
            timeout=30,
            refresh_strategy="lazy",  # can be "lazy" or "background"
        )
        return connector
    
    def get_mssql_engine(self):
        """Creates and returns a SQLAlchemy engine for MSSQL using Cloud SQL Connector."""
        try:
            conn = self.__connector.connect(
                config.mssql_instance,
                "pytds",
                user=config.mssql_user,
                password=config.mssql_password,
                db=config.mssql_database
            )

            engine = sqlalchemy.create_engine(
                "mssql+pytds://",
                creator=conn,
            )

            self.__logger.info("MSSQL engine created successfully.")

        except Exception as e:
            self.__logger.error(f"Error creating MSSQL engine: {e}")
            raise e

        return engine

    def close_mssql_connector(self):
        """Closes the Cloud SQL Connector."""
        if self.__connector:
            self.__connector.close()
            self.__logger.info("MSSQL Connector closed.")
        else:
            self.__logger.warning("MSSQL Connector was not initialized.")
    
    def test_bigquery_connection(self):
        """Tests the database connections and logs the results."""
        try:
            # Test BigQuery connection
            pandas_gbq.read_gbq("SELECT 1", project_id=os.getenv("BIGQUERY_PROJECT_ID", "default_project"))
            self.__logger.info("BigQuery connection successful.")
        except Exception as e:
            self.__logger.error(f"BigQuery connection failed: {e}")


    def main(self):
        """Returns both connection strings as a tuple."""
        self.test_bigquery_connection()
        return self.__bigquery_conn_str, self.__mssql_engine