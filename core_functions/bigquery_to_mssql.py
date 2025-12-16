import os
import config
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
            
        except Exception as e:
            self.__logger.error(f"Error creating tables: {e}")
            raise e
        
    def __get_biquery_data(self, table_name=''):
        """Get data from BigQuery based on last run timestamp."""
        try:
            if table_name == 'CustomerPOULBQ':
                query = "SELECT * FROM `{}.stg_customer_po_ul`".format(config.bigquery_dataset_id)
                if self.__last_run_timestamp:
                    query += f" WHERE update_date > '{self.__last_run_timestamp}'"
                
                df = pandas_gbq.read_gbq(
                    query,
                    project_id=config.bigquery_project_id,
                    dialect='standard'
                )
                self.__logger.info(f"Fetched {len(df)} records from BigQuery.")
                return df
            elif table_name == 'CustomerPOULDetailBQ':
                query = "SELECT * FROM `{}.stg_customer_po_ul_detail`".format(config.bigquery_dataset_id)
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
        except Exception as e:
            self.__logger.error(f"Error fetching data from BigQuery: {e}")
            raise e
    
    def __rename_columns(self, table_name, df):
        """Rename columns in the DataFrame to match MSSQL table schema."""
        column_mapping = { }
        
        if table_name == 'CustomerPOULBQ':
            column_mapping = {
                'customer_id': 'customerId',
                'po_ref_number': 'poRefNumber',
                'company_id': 'companyid',
                'warehouse_id': 'warehouseid',
                'po_date': 'poDate',
                'delivery_date': 'deliveryDate',
                'cancellation_date': 'cancellationDate',
                'customer_branch_id': 'customerBranchId',
                'customer_branch_name': 'customerBranchName',
                'customer_branch_lookup_code': 'customerBranchLookUpCode',
                'remark': 'remark',
                'customer_po_id': 'customerPOId',
                'po_status': 'poStatus',
                'manual_encoded': 'manualEncoded',
                'create_by': 'createBy',
                'create_date': 'createDate',
                'update_by': 'updateBy',
                'update_date': 'updateDate',
                'cancel_by': 'cancelBy',
                'cancel_date': 'cancelDate',
                'cancel_reason': 'cancelReason'
            }

        elif table_name == 'CustomerPOULDetailBQ':
            column_mapping = {
                'customer_id': 'customerId',
                'po_ref_number': 'poRefNumber',
                'product_id': 'productId',
                'sku_id': 'skuId',
                'customer_sku_code': 'customerSKUCode',
                'customer_sku_desc': 'customerSKUDesc',
                'unit_price': 'unitPrice',
                'discount_percent': 'discountPercent',
                'net_price': 'netPrice'
            }

        return df.rename(columns=column_mapping)
        
    def main(self):
        """Main method to return connection strings."""
        self.__logger.info("BigQuery to MSSQL Bridge initialized.")
        retval = ''

        self.__logger.info("Creating landing tables if not exist...")
        self.__create_landing_tables()

        self.__logger.info("Fetching data from BigQuery...")
        customer_po_ul_bq = self.__get_biquery_data('CustomerPOULBQ')
        customer_po_ul_detail_bq = self.__get_biquery_data('CustomerPOULDetailBQ')
        
        self.__logger.info("Renaming columns to match MSSQL schema...")
        customer_po_ul_bq = self.__rename_columns('CustomerPOULBQ', customer_po_ul_bq)
        customer_po_ul_detail_bq = self.__rename_columns('CustomerPOULDetailBQ', customer_po_ul_detail_bq)
        
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