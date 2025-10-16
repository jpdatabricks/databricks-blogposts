"""
Lakebase PostgreSQL Client for Databricks
==========================================

This module provides a client for connecting to and writing features
to Databricks Lakebase (PostgreSQL-compatible OLTP database).

IMPORTANT: In this project, "Lakebase" ALWAYS refers to Lakebase PostgreSQL
(OLTP database at port 5432), NOT Delta Lake or any other storage layer.
See .cursorrules for project conventions.

Author: Databricks
Date: October 2025
"""

import psycopg2
from psycopg2.extras import execute_batch
import logging
from typing import Dict, List, Optional
import pandas as pd
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LakebaseClient:
    """
    Client for Databricks Lakebase (PostgreSQL OLTP database)
    """
    
    def __init__(self, host: str, port: int = 5432, database: str = "feature_store",
                 user: str = "token", password: Optional[str] = None):
        """
        Initialize Lakebase client
        
        Args:
            host: Lakebase hostname (e.g., workspace.cloud.databricks.com)
            port: PostgreSQL port (default: 5432)
            database: Database name
            user: Username (use 'token' for OAuth)
            password: OAuth token or password
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self._connection = None
        
        logger.info(f"Initialized LakebaseClient for {host}:{port}/{database}")
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections
        
        Yields:
            psycopg2 connection object
        """
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=10,
                sslmode='require'
            )
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def test_connection(self) -> bool:
        """
        Test the connection to Lakebase
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                cursor.close()
                logger.info("✅ Lakebase connection test successful")
                return result[0] == 1
        except Exception as e:
            logger.error(f"❌ Lakebase connection test failed: {e}")
            return False
    
    def create_feature_table(self, table_name: str = "transaction_features"):
        """
        Create the transaction features table in Lakebase
        
        Args:
            table_name: Name of the table to create
        """
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            -- Primary keys
            transaction_id VARCHAR(50) PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            
            -- Original transaction data
            user_id VARCHAR(50) NOT NULL,
            merchant_id VARCHAR(50),
            amount DOUBLE PRECISION,
            currency VARCHAR(10),
            merchant_category VARCHAR(50),
            payment_method VARCHAR(50),
            ip_address VARCHAR(50),
            device_id VARCHAR(50),
            location_lat DOUBLE PRECISION,
            location_lon DOUBLE PRECISION,
            card_type VARCHAR(20),
            
            -- Time-based features
            hour INTEGER,
            day_of_week INTEGER,
            is_business_hour INTEGER,
            is_weekend INTEGER,
            hour_sin DOUBLE PRECISION,
            hour_cos DOUBLE PRECISION,
            
            -- Amount-based features
            amount_log DOUBLE PRECISION,
            amount_sqrt DOUBLE PRECISION,
            
            -- Velocity features
            user_txn_count_1h INTEGER,
            user_amount_sum_1h DOUBLE PRECISION,
            merchant_txn_count_1h INTEGER,
            
            -- Processing metadata
            processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create indexes for performance
        CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp ON {table_name}(timestamp);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_user_id ON {table_name}(user_id);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_merchant_id ON {table_name}(merchant_id);
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(create_table_sql)
                cursor.close()
                logger.info(f"✅ Created table: {table_name}")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise
    
    def write_batch(self, df: pd.DataFrame, table_name: str = "transaction_features",
                   batch_size: int = 1000):
        """
        Write a pandas DataFrame to Lakebase in batches
        
        Args:
            df: Pandas DataFrame with features
            table_name: Target table name
            batch_size: Number of rows per batch
        """
        if df.empty:
            logger.warning("Empty DataFrame, skipping write")
            return
        
        # Prepare column names and placeholders
        columns = list(df.columns)
        placeholders = ','.join(['%s'] * len(columns))
        insert_sql = f"""
            INSERT INTO {table_name} ({','.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT (transaction_id) DO UPDATE SET
            {','.join([f"{col}=EXCLUDED.{col}" for col in columns if col != 'transaction_id'])}
        """
        
        # Convert DataFrame to list of tuples
        data = [tuple(row) for row in df.values]
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                execute_batch(cursor, insert_sql, data, page_size=batch_size)
                cursor.close()
                logger.info(f"✅ Wrote {len(df)} rows to {table_name}")
        except Exception as e:
            logger.error(f"Error writing batch: {e}")
            raise
    
    def write_streaming_batch(self, batch_df, batch_id: int, table_name: str = "transaction_features"):
        """
        Write a streaming micro-batch to Lakebase
        
        This function is designed to be used with foreachBatch in PySpark Structured Streaming
        
        Args:
            batch_df: PySpark DataFrame (micro-batch)
            batch_id: Batch ID from streaming query
            table_name: Target table name
        """
        if batch_df.isEmpty():
            logger.info(f"Batch {batch_id}: Empty, skipping")
            return
        
        logger.info(f"Processing batch {batch_id}...")
        
        # Convert to Pandas
        pandas_df = batch_df.toPandas()
        
        # Write to Lakebase
        self.write_batch(pandas_df, table_name)
        
        logger.info(f"✅ Batch {batch_id} complete: {len(pandas_df)} rows written")
    
    def read_features(self, query: str) -> pd.DataFrame:
        """
        Read features from Lakebase using SQL query
        
        Args:
            query: SQL query to execute
            
        Returns:
            Pandas DataFrame with results
        """
        try:
            with self.get_connection() as conn:
                df = pd.read_sql(query, conn)
                logger.info(f"✅ Read {len(df)} rows from Lakebase")
                return df
        except Exception as e:
            logger.error(f"Error reading features: {e}")
            raise
    
    def get_recent_features(self, user_id: str, hours: int = 24,
                           table_name: str = "transaction_features") -> pd.DataFrame:
        """
        Get recent features for a user
        
        Args:
            user_id: User ID to query
            hours: Number of hours to look back
            table_name: Table name
            
        Returns:
            Pandas DataFrame with features
        """
        query = f"""
            SELECT *
            FROM {table_name}
            WHERE user_id = '{user_id}'
            AND timestamp > NOW() - INTERVAL '{hours} hours'
            ORDER BY timestamp DESC
        """
        return self.read_features(query)
    
    def get_table_stats(self, table_name: str = "transaction_features") -> Dict:
        """
        Get statistics about the feature table
        
        Args:
            table_name: Table name
            
        Returns:
            Dictionary with table statistics
        """
        query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(DISTINCT merchant_id) as unique_merchants,
                MIN(timestamp) as earliest_timestamp,
                MAX(timestamp) as latest_timestamp,
                AVG(amount) as avg_amount
            FROM {table_name}
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                result = cursor.fetchone()
                cursor.close()
                
                stats = {
                    'total_rows': result[0],
                    'unique_users': result[1],
                    'unique_merchants': result[2],
                    'earliest_timestamp': result[3],
                    'latest_timestamp': result[4],
                    'avg_amount': result[5]
                }
                
                logger.info(f"📊 Table stats: {stats['total_rows']:,} rows")
                return stats
        except Exception as e:
            logger.error(f"Error getting table stats: {e}")
            raise


def get_lakebase_client_from_secrets(spark=None) -> LakebaseClient:
    """
    Create a LakebaseClient using Databricks secrets
    
    Args:
        spark: SparkSession (for accessing dbutils)
        
    Returns:
        Initialized LakebaseClient
    """
    try:
        # Try to get dbutils
        if spark:
            dbutils = spark._jvm.com.databricks.backend.daemon.driver.DriverLocal.toScalaDriverLocal(
                spark._jvm.org.apache.spark.TaskContext.get()
            ).driverLocal().dbutils()
        else:
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            dbutils = DBUtils(spark)
        
        # Get secrets
        host = dbutils.secrets.get(scope="lakebase", key="host")
        password = dbutils.secrets.get(scope="lakebase", key="token")
        database = dbutils.secrets.get(scope="lakebase", key="database")
        
        return LakebaseClient(
            host=host,
            port=5432,
            database=database,
            user="token",
            password=password
        )
    except Exception as e:
        logger.error(f"Error creating client from secrets: {e}")
        logger.info("Falling back to manual configuration")
        raise


# Example usage
if __name__ == "__main__":
    # Example configuration
    client = LakebaseClient(
        host="your-workspace.cloud.databricks.com",
        port=5432,
        database="feature_store",
        user="token",
        password="your-oauth-token"
    )
    
    # Test connection
    if client.test_connection():
        print("✅ Connected to Lakebase!")
        
        # Create table
        client.create_feature_table()
        
        # Get stats
        stats = client.get_table_stats()
        print(f"Table stats: {stats}")

