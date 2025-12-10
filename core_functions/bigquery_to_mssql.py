import os
import config
import pandas_gbq
import sqlalchemy
from dbconn import DbConn
from datetime import datetime
from sqlalchemy import Column, Float, Integer, String, Table, MetaData, DateTime, Date


class BigQueryToMSSQL(object):
    """Class to Copy data from Bigquery to SQL server.
    
    TO DO:
    - function to create table in query if not exists
    - function to get latest timestamp from mssql table
    - function to query data from bigquery based on timestamp
    - function to insert data into mssql table
    
    """

    def __init__(self, logger):
        """Class Init."""
        self.__logger = logger
        self.__dbconn = DbConn(logger)
        self.__bigquery_conn_str = self.__dbconn.get_bigquery_connection_string()
        self.__mssql_engine = self.__dbconn.get_mssql_engine()
        self.__last_run_timestamp = None

    def __get_last_run_timestamp(self):
        """Get the last run timestamp from MSSQL table."""
        # Example logic to get the last run timestamp
        try:
            with self.__mssql_engine.connect() as connection:
                result = connection.execute(
                    "SELECT MAX(update_date) FROM CustomerPOULBQ"
                )
                self.__last_run_timestamp = result.scalar()
                self.__logger.info(f"Last run timestamp: {self.__last_run_timestamp}")
        except Exception as e:
            self.__logger.error(f"Error fetching last run timestamp: {e}")
            raise e

    def __create_landing_tables(self):
        """Create necessary tables in MSSQL if they do not exist."""
        # Example table creation logic
        inspector = sqlalchemy.inspect(self.__mssql_engine)
        metadata = sqlalchemy.MetaData()
        
        try:
            if not inspector.has_table('CustomerPOULBQ'):
                customer_poul_bq = Table(
                    'CustomerPOULBQ',
                    metadata,
                    Column('customerId', Integer, primary_key=True),
                    Column('poRefNumber', String(50), primary_key=True),
                    Column('companyid', Integer),
                    Column('warehouseid', Integer),
                    Column('poDate', Date),
                    Column('deliveryDate', Date),
                    Column('cancellationDate', Date),
                    Column('customerBranchId', Integer),
                    Column('customerBranchName', String(250)),
                    Column('customerBranchLookUpCode', String(50)),
                    Column('remark', String(250)),
                    Column('customerPOId', Integer),
                    Column('poStatus', String(20)),
                    Column('manualEncoded', Integer),
                    Column('createBy', String(50)),
                    Column('createDate', Date),
                    Column('updateBy', String(50)),
                    Column('updateDate', Date),
                    Column('cancelBy', String(50)),
                    Column('cancelDate', Date),
                    Column('cancelReason', String(250)),
                )
            else:
                self.__get_last_run_timestamp()
            
            if not inspector.has_table('CustomerPOULDetailBQ'):
                customer_poul_detail_bq = Table(
                    'CustomerPOULDetailBQ',
                    metadata,
                    Column('customerId', Integer, primary_key=True),
                    Column('poRefNumber', String(50), primary_key=True),
                    Column('productId', Integer),
                    Column('skuId', Integer),
                    Column('customerSKUCode', String(50)),
                    Column('customerSKUDesc', String(250)),
                    Column('unitPrice', Float),
                    Column('discountPercent', Float),
                    Column('netPrice', Float),
                    Column('cancelDate', Date),
                    Column('updateDate', Date),
                )   
            else:
                self.__logger.info("Table 'CustomerPOULDetailBQ' already exists. No action taken.")    
            
        except Exception as e:
            self.__logger.error(f"Error creating tables: {e}")
            raise e
        
    def main(self):
        """Main method to return connection strings."""
        self.__logger.info("BigQuery to MSSQL Bridge initialized.")
        retval = ''




        return retval