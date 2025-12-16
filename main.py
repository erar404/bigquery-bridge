import os
import logging
from logger import LogHandler
from datetime import datetime
from dbconn import DbConn
from core_functions.bigquery_to_mssql import BigQueryToMSSQL

if __name__ == "__main__":
    # Set up logging
    LogHandler('BigQueryBridge')
    start_time = datetime.now()
    logging.info(f"Script started at: {start_time}")
    logger = logging.getLogger('BigQueryBridge')

    # Initialize The Main Function Class
    bq_to_mssql = BigQueryToMSSQL(logger) 
    bq_to_mssql.main()

    # Get current date and time
    now = datetime.now()
    formatted_date = now.strftime("%Y-%m-%d %H:%M:%S")
    end_time = datetime.now()

    total_time = end_time - start_time
    logger.info(f"Total execution time: {total_time}")
    logger.info("Script execution completed.")
