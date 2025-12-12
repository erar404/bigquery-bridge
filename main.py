import os
import logging
from logger import LogHandler
from datetime import datetime
from dbconn import DbConn
from core_functions.bigquery_to_mssql import BigQueryToMSSQL

if __name__ == "__main__":
    # Set up logging
    LogHandler('BigQueryBridge')
    logger = logging.getLogger('BigQueryBridge')

    # Test Database connections
    # db_conn = DbConn(logger)
    # bigquery_conn_str, mssql_conn_str = db_conn.main()
    # db_conn.close_mssql_connector()

    # logger.info(f'MSSQL Conn: {mssql_conn_str}')
    # logger.info('------------------------')
    # logger.info(f'BQ Conn: {bigquery_conn_str}')

    # Initialize The Main Function Class
    bq_to_mssql = BigQueryToMSSQL(logger) 
    bq_to_mssql.main()

    # Get current working directory
    cwd = os.getcwd()
    logger.info(f"Current working directory: {cwd}")

    # Get current date and time
    now = datetime.now()
    formatted_date = now.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Current date and time: {formatted_date}")

