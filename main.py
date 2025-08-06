#!/usr/bin/env python3
"""
ETL Script for Cases Data Migration
Extracts data from CaseTrackingSystemDb and loads into DataWarehouse
Performs full sync with insert/update/delete operations
"""

import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import urllib
import logging
from datetime import datetime


def setup_logging():
    """Configure logging for the ETL process"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'etl_cases_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def extract():
    """
    Extract data from the source database (CaseTrackingSystemDb)
    
    Returns:
        pandas.DataFrame: Extracted cases data
    """
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Starting data extraction...")
        
        # Source database connection parameters
        uid = 'moaz.mahmoud'
        pwd = 'Ez0K3i5&(£683'
        driver = '{ODBC Driver 18 for SQL Server}'
        server = '40.114.119.44,1433'
        database = 'CaseTrackingSystemDb'

        # Build connection string
        connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={quote_plus(pwd)};Encrypt=yes;TrustServerCertificate=yes'
        connection_url = f"mssql+pyodbc:///?odbc_connect={connection_string}"
        
        # Create engine and extract data
        src_engine = create_engine(connection_url, pool_pre_ping=True)
        
        with src_engine.connect() as src_conn:
            query = """
            SELECT 
                Cases.Name AS Name,
                Cases.PhoneNumber1,
                Cases.CaseNumber AS NationalId,
                Cases.City,
                CASE
                    WHEN Cases.Gender = 20 THEN N'أنثى'
                    WHEN Cases.Gender = 10 THEN N'ذكر' 
                END AS Gender,			
                Cases.FamilyNumber,
                Cases.Age
            FROM 
                Cases
            """
            
            df = pd.read_sql_query(query, src_conn)
            logger.info(f"Successfully extracted {len(df)} records")
            logger.info("First 5 rows of the extracted data:")
            logger.info(f"\n{df.head()}")
            
            return df
            
    except Exception as e:
        logger.error(f"Data extraction failed: {e}")
        return pd.DataFrame()


def load(df, table_name):
    """
    Load data to the target database (DataWarehouse) using full sync approach
    
    Args:
        df (pandas.DataFrame): Data to load
        table_name (str): Target table name
    """
    logger = logging.getLogger(__name__)
    
    try:
        if df.empty:
            logger.warning("No data to load — DataFrame is empty.")
            return

        logger.info(f"Starting data load to table: {table_name}")
        
        # Target database connection parameters
        server = '40.114.119.44,1433'
        database = 'DataWarehouse'
        uid = 'rawan.abdullah'
        pwd = '2[R8or9<jWCS'

        # Build connection string
        params = urllib.parse.quote_plus(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={uid};"
            f"PWD={pwd};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"charset=UTF-8"
        )

        engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}', pool_pre_ping=True)
        staging_table = f"{table_name}_staging"

        # Load to staging table
        logger.info(f"Loading {len(df)} records to staging table: {staging_table}")
        df.to_sql(staging_table, engine, if_exists='replace', index=False)

        # Perform MERGE operation
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                logger.info("Executing MERGE operation (insert/update)...")
                merge_sql = f"""
                MERGE {table_name} AS target
                USING {staging_table} AS source
                ON target.NationalId = source.NationalId
                WHEN MATCHED THEN 
                    UPDATE SET 
                        target.Name = source.Name,
                        target.PhoneNumber1 = source.PhoneNumber1,
                        target.City = source.City,
                        target.Gender = source.Gender,
                        target.FamilyNumber = source.FamilyNumber,
                        target.Age = source.Age
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (Name, PhoneNumber1, NationalId, City, Gender, FamilyNumber, Age)
                    VALUES (source.Name, source.PhoneNumber1, source.NationalId, source.City, source.Gender, source.FamilyNumber, source.Age);
                """
                result = conn.execute(text(merge_sql))
                logger.info(f"MERGE operation affected {result.rowcount} rows")

                logger.info("Executing DELETE operation for missing records...")
                delete_sql = f"""
                DELETE FROM {table_name}
                WHERE NationalId NOT IN (SELECT NationalId FROM {staging_table});
                """
                result = conn.execute(text(delete_sql))
                logger.info(f"DELETE operation affected {result.rowcount} rows")

                trans.commit()
                logger.info("Full sync completed successfully: insert/update/delete operations applied.")
                
            except Exception as e:
                trans.rollback()
                logger.error(f"Transaction failed, rolling back: {e}")
                raise

    except Exception as e:
        logger.error(f"Data load failed: {e}")
        raise


def run_etl():
    """Main ETL process orchestrator"""
    logger = setup_logging()
    
    try:
        logger.info("=" * 50)
        logger.info("Starting ETL Process for Cases Data")
        logger.info("=" * 50)
        
        start_time = datetime.now()
        
        # Extract data
        df = extract()
        
        if df.empty:
            logger.warning("No data extracted. ETL process terminated.")
            return
        
        # Load data
        load(df, 'Cases')
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("=" * 50)
        logger.info(f"ETL Process completed successfully in {duration}")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise


if __name__ == "__main__":
    run_etl()