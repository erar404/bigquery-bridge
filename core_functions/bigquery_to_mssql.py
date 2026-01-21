import os
import config
import mapping
import pandas
import pandas_gbq
import sqlalchemy
from dbconn import DbConn
from datetime import datetime
from sqlalchemy import Column, Float, Integer, String, Table, MetaData, DateTime, Date, text


class BigQueryToMSSQL(object):
    """Class to Copy data from Bigquery to SQL server.
    Attributes:
        logger: Logger object for logging information and errors. 
    """

    def __init__(self, logger):
        """Class Init."""
        self.__logger = logger
        self.__dbconn = DbConn(logger)
        self.__bigquery_conn_str = self.__dbconn.get_bigquery_connection_string()
        self.__mssql_engine = self.__dbconn.main()
        self.__last_run_timestamp = None

    def __get_last_run_timestamp(self):
        """Get the last run timestamp from MSSQL table."""
        try:
            with self.__mssql_engine.connect() as connection:
                result = connection.execute(
                    text("SELECT MAX(updateDate) FROM CustomerPOULBQ")
                )
                self.__last_run_timestamp = result.scalar()
                self.__logger.info(f"Last run timestamp: {self.__last_run_timestamp}")
            
        except Exception as e:
            self.__logger.error(f"Error fetching last run timestamp: {e}")
            raise e

    def __create_landing_tables(self):
        """Create necessary tables in MSSQL if they do not exist."""
        inspector = sqlalchemy.inspect(self.__mssql_engine)
        metadata = sqlalchemy.MetaData()
        self.__logger.info("Creating landing tables if they do not exist...")
        
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

            if not inspector.has_table('DocumentAIBQ'):
                metadata.create_all(self.__mssql_engine)
                self.__logger.info("Table 'DocumentAIBQ' created successfully.")
            else:
                self.__logger.info("Table 'DocumentAIBQ' already exists. No action taken.")   

            if not inspector.has_table('DocumentAIDetailBQ'):
                metadata.create_all(self.__mssql_engine)
                self.__logger.info("Table 'DocumentAIDetailBQ' created successfully.")    
            else:
                self.__logger.info("Table 'DocumentAIDetailBQ' already exists. No action taken.")
                self.__get_last_run_timestamp()   
            
        except Exception as e:
            self.__logger.error(f"Error creating tables: {e}")
            raise e
        
    def __get_biquery_data(self, table_name=''):
        """Get data from BigQuery based on last run timestamp."""
        try:
            if table_name == 'CustomerPOULBQ':
                query = "SELECT * FROM `{}.stg_document_ai`".format(config.bigquery_dataset_id)
                if self.__last_run_timestamp:
                    query += f" WHERE created_at  > '{self.__last_run_timestamp}'"
                
                df = pandas_gbq.read_gbq(
                    query,
                    project_id=config.bigquery_project_id,
                    dialect='standard'
                )
                self.__logger.info(f"Fetched {len(df)} records from BigQuery.")
                return df
            elif table_name == 'CustomerPOULDetailBQ':
                query = "SELECT * FROM `{}.stg_document_ai_detail`".format(config.bigquery_dataset_id)
                
                if self.__last_run_timestamp:
                    query = """
                    SELECT * FROM `{}.stg_customer_po_ul_detail` where po_ref_number IN (
                        SELECT po_ref_number FROM `{}.stg_customer_po_ul` WHERE update_date > '{}'
                    )
                    """.format(config.bigquery_dataset_id, 
                               config.bigquery_dataset_id,
                               self.__last_run_timestamp)
                
                df = pandas_gbq.read_gbq(
                    query,
                    project_id=config.bigquery_project_id,
                    dialect='standard'
                )
                self.__logger.info(f"Fetched {len(df)} records from BigQuery.")
                return df
            elif table_name == 'DocumentAIBQ':
                query = "SELECT * FROM `{}.stg_document_ai`".format(config.bigquery_dataset_id)
                
                if self.__last_run_timestamp:
                    query += f" WHERE created_at  > '{self.__last_run_timestamp}'"
                
                df = pandas_gbq.read_gbq(
                    query,
                    project_id=config.bigquery_project_id,
                    dialect='standard'
                )
                self.__logger.info(f"Fetched {len(df)} records from BigQuery.")
                return df
            elif table_name == 'DocumentAIDetailBQ':
                query = "SELECT * FROM `{}.stg_document_ai_detail`".format(config.bigquery_dataset_id)
                
                if self.__last_run_timestamp:
                    query += f" WHERE created_at  > '{self.__last_run_timestamp}'"
                
                df = pandas_gbq.read_gbq(
                    query,
                    project_id=config.bigquery_project_id,
                    dialect='standard'
                )
                self.__logger.info(f"Fetched {len(df)} records from BigQuery.")
                return df
        except Exception as e:
            self.__logger.error(f"Error fetching data from BigQuery: {e}")
            raise e
        
   
    def __rename_columns(self, table_name, df):
        """Rename columns in the DataFrame to match MSSQL table schema."""
        column_mapping = mapping.table_mapping.get(table_name, {})
        ignore_cols = mapping.ignore_columns.get(table_name, [])
        column_defaults = mapping.column_defaults.get(table_name, {})
        requirements_cols = mapping.required_columns.get(table_name, [])
        date_cols = mapping.date_columns.get(table_name, [])

        if not column_mapping:
            self.__logger.warning(f"No column mapping found for table: {table_name}")
            return df
        
        if ignore_cols:
            df.drop(columns=ignore_cols, inplace=True)
            self.__logger.info(f"Removed Columns for {table_name}")

        if requirements_cols:
            self.__logger.info('Removing Duplicates for table: {}'.format(table_name))
            df.drop_duplicates(requirements_cols, keep='last', inplace=True)

        for key, val in column_defaults.items():
            df[key] = df[key].mask(df[key].isnull(), val)

        for date_col in date_cols:
            if date_col in df.columns:
                df[date_col] = pandas.to_datetime(df[date_col], errors="raise", format='mixed', yearfirst=True, dayfirst=True)
                df[date_col] = df[date_col].dt.strftime("%Y-%m-%d")

        return df.rename(columns=column_mapping)
        
    def main(self):
        """Main method to return connection strings."""
        self.__logger.info("BigQuery to MSSQL Bridge initialized.")
        retval = ''
        self.__logger.info("Creating landing tables if not exist...")
        # self.__create_landing_tables()        #   disabled. Tables are pre-created. Enable if dynamic creation is needed.
        self.__get_last_run_timestamp()
        self.__logger.info(f"Last run timestamp obtained: {self.__last_run_timestamp}")

        self.__logger.info("Fetching data from BigQuery...")
        customer_po_ul_bq = self.__get_biquery_data('DocumentAIBQ')
        customer_po_ul_detail_bq = self.__get_biquery_data('DocumentAIDetailBQ')
        
        self.__logger.info("Renaming columns to match MSSQL schema...")
        customer_po_ul_bq = self.__rename_columns('customerpoul_v2', customer_po_ul_bq)
        customer_po_ul_detail_bq = self.__rename_columns('customerpouldetail_v2', customer_po_ul_detail_bq)

        self.__logger.info("Inserting data into MSSQL...")
        try:
            with self.__mssql_engine.begin() as connection:
                customer_po_ul_bq.to_sql(
                    'CustomerPOULBQ',
                    con=connection,
                    if_exists='append',
                    index=False
                )

                customer_po_ul_detail_bq.to_sql(
                    'CustomerPOULDetailBQ',
                    con=connection,
                    if_exists='append',
                    index=False
                )
            self.__logger.info("Data inserted successfully into MSSQL.")
        except Exception as e:
            self.__logger.error(f"Error inserting data into MSSQL: {e}")
            raise e
        
        return retval