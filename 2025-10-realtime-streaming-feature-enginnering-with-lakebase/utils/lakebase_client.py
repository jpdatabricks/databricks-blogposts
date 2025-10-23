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
from psycopg2 import OperationalError, DatabaseError
from psycopg2.extras import execute_batch
import logging
from typing import Dict, List, Optional
import pandas as pd
from contextlib import contextmanager
import databricks.sdk
from databricks.sdk import WorkspaceClient
import uuid
import time



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LakebaseClient:
    """
    Client for Databricks Lakebase (PostgreSQL OLTP database)
    """
    
    def __init__(self, instance_name, 
                 database: str = "databricks_postgres"):
        """
        Initialize Lakebase client
        
        Args:
            instance_name: Lakebase instance name
            database: Database name. databricks_postgres is the default database.            
        """
        self.instance_name = instance_name
        self.database = database
        
    def get_credentials(self):
        """
        Get Databricks Lakebase credentials
        
        Yields:
            lakebase database credentials
        """
        conn = None
        try:
            w = WorkspaceClient()
            print(databricks.sdk.version.__version__)
            host = w.database.get_database_instance(name=self.instance_name).read_write_dns
            port = 5432
            cred = w.database.generate_database_credential(
                request_id=str(uuid.uuid4()), instance_names=[self.instance_name])            
            user = w.current_user.me().user_name
            password = cred.token
            return {
                "host": host,
                "port": port,
                "user": user,
                "password": password
            }
        except Exception as e:
            logger.error(f"Unable to get Lakebase credentials: {e}") 
            raise



    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections
        
        Yields:
            psycopg2 connection object
        """
        conn = None
        try:
            creds = self.get_credentials()
            
            conn = psycopg2.connect(
                host=creds['host'],
                port=creds['port'],
                dbname=self.database,
                user=creds['user'],
                password=creds['password'],
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
                logger.info("Lakebase connection test successful")
                return result[0] == 1
        except Exception as e:
            logger.error(f"Lakebase connection test failed: {e}")
            return False
    
    def create_feature_table(self, table_name: str = "transaction_features"):
        """
        Create the unified feature table in Lakebase PostgreSQL.
        
        This table combines BOTH stateless transaction features AND stateful fraud detection features
        in a single, comprehensive schema optimized for real-time ML model serving.
        
        Use Cases:
        - 01_streaming_features.ipynb: Stateless feature engineering (populates ~40 columns)
        - 02_stateful_fraud_detection.ipynb: Stateful fraud detection (populates all ~70 columns)
        
        Table Schema (~70+ columns total):
        
        **Stateless Features (~40 columns):**
        - Time-based: year, month, day, hour, cyclical encodings, business hour flags
        - Amount-based: log, sqrt, squared, categories, round amount flags
        - Merchant: risk scores, category risk levels
        - Location: high-risk flags, international, region
        - Device: device type, has_device_id flag
        - Network: Tor/private IP detection, IP class
        
        **Stateful Features (~25 columns):**
        - Velocity: transaction counts in time windows
        - IP tracking: IP change detection and counts
        - Location anomalies: distance from last, velocity (km/h)
        - Amount anomalies: ratios, z-scores
        - Fraud indicators: rapid transactions, impossible travel, amount anomalies
        - Composite: fraud_score (0-100), is_fraud_prediction (binary)
        
        **Metadata (~5 columns):**
        - created_at, processing_timestamp
        
        Args:
            table_name: Name of the table to create (default: transaction_features)
                       Common names: "transaction_features", "fraud_features"
        
        Benefits:
        - Single source of truth for all features
        - No joins needed for model inference (<10ms latency)
        - Unified schema for all notebooks
        - Easier to maintain and evolve
        """
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            -- Primary keys
            transaction_id VARCHAR(100) PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            
            -- Original transaction data
            user_id VARCHAR(100) NOT NULL,
            merchant_id VARCHAR(100),
            amount DOUBLE PRECISION NOT NULL,
            currency VARCHAR(10),
            merchant_category VARCHAR(50),
            payment_method VARCHAR(50),
            ip_address VARCHAR(50),
            device_id VARCHAR(50),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            card_type VARCHAR(20),
            
            -- Time-based features (stateless - from FeatureEngineer)
            year INTEGER,
            month INTEGER,
            day INTEGER,
            hour INTEGER,
            minute INTEGER,
            day_of_week INTEGER,
            day_of_year INTEGER,
            week_of_year INTEGER,
            is_business_hour INTEGER,
            is_weekend INTEGER,
            is_holiday INTEGER,
            is_night INTEGER,
            is_early_morning INTEGER,
            hour_sin DOUBLE PRECISION,
            hour_cos DOUBLE PRECISION,
            day_of_week_sin DOUBLE PRECISION,
            day_of_week_cos DOUBLE PRECISION,
            month_sin DOUBLE PRECISION,
            month_cos DOUBLE PRECISION,
            
            -- Amount-based features (stateless - from FeatureEngineer)
            amount_log DOUBLE PRECISION,
            amount_sqrt DOUBLE PRECISION,
            amount_squared DOUBLE PRECISION,
            amount_category VARCHAR(20),
            is_round_amount INTEGER,
            is_exact_amount INTEGER,
            
            -- Merchant features (stateless - from FeatureEngineer)
            merchant_risk_score DOUBLE PRECISION,
            merchant_category_risk VARCHAR(20),
            
            -- Location features (stateless - from FeatureEngineer)
            is_high_risk_location INTEGER,
            is_international INTEGER,
            location_region VARCHAR(20),
            
            -- Device features (stateless - from FeatureEngineer)
            has_device_id INTEGER,
            device_type VARCHAR(20),
            
            -- Network features (stateless - from FeatureEngineer)
            is_tor_ip INTEGER,
            is_private_ip INTEGER,
            ip_class VARCHAR(20),
            
            -- ============================================
            -- STATEFUL FRAUD DETECTION FEATURES
            -- (from FraudDetectorProcessor)
            -- ============================================
            
            -- Velocity features (stateful)
            user_transaction_count INTEGER,
            transactions_last_hour INTEGER,
            transactions_last_10min INTEGER,
            
            -- IP tracking features (stateful)
            ip_changed INTEGER,
            ip_change_count_total INTEGER,
            
            -- Location anomaly features (stateful)
            distance_from_last_km DOUBLE PRECISION,
            velocity_kmh DOUBLE PRECISION,
            
            -- Amount anomaly features (stateful)
            amount_vs_user_avg_ratio DOUBLE PRECISION,
            amount_vs_user_max_ratio DOUBLE PRECISION,
            amount_zscore DOUBLE PRECISION,
            
            -- Time features (stateful)
            seconds_since_last_transaction DOUBLE PRECISION,
            
            -- Fraud indicators (stateful)
            is_rapid_transaction INTEGER,
            is_impossible_travel INTEGER,
            is_amount_anomaly INTEGER,
            
            -- Composite fraud score and prediction (stateful)
            fraud_score DOUBLE PRECISION,
            is_fraud_prediction INTEGER,
            
            -- Processing metadata
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create indexes for performance
        CREATE INDEX IF NOT EXISTS idx_{table_name}_user_id ON {table_name}(user_id);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp ON {table_name}(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_merchant_id ON {table_name}(merchant_id);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_merchant_category ON {table_name}(merchant_category);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_fraud_score ON {table_name}(fraud_score DESC);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_fraud_prediction ON {table_name}(is_fraud_prediction);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_device_id ON {table_name}(device_id);
        """
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(create_table_sql)
                cursor.close()
                logger.info(f"Created unified feature table: {table_name} (~70+ columns)")
        except Exception as e:
            logger.error(f"Error creating feature table: {e}")
            raise
    
    def write_streaming_batch(self, batch_df, batch_id: int, table_name: str = "transaction_features", 
                             batch_size: int = 10):
        """
        Write a streaming micro-batch to Lakebase
        
        This function is designed to be used with foreachBatch in PySpark Structured Streaming
        
        Args:
            batch_df: PySpark DataFrame (micro-batch)
            batch_id: Batch ID from streaming query
            table_name: Target table name
            batch_size: Number of rows per batch for insert
        """
        logger.info(f"Processing batch {batch_id}...")
        
        pandas_df = batch_df.toPandas()
        
        if pandas_df.empty:
            logger.warning(f"Batch {batch_id}: Empty DataFrame, skipping")
            return
        
        columns = list(pandas_df.columns)
        placeholders = ','.join(['%s'] * len(columns))
        insert_sql = f"""
            INSERT INTO {table_name} ({','.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT (transaction_id) DO UPDATE SET
            {','.join([f"{col}=EXCLUDED.{col}" for col in columns if col != 'transaction_id'])}
        """
        
        data = [tuple(row) for row in pandas_df.values]
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                execute_batch(cursor, insert_sql, data, page_size=batch_size)
                cursor.close()
                logger.info(f"Batch {batch_id} complete: {len(pandas_df)} rows written")
        except Exception as e:
            logger.error(f"Error writing batch {batch_id}: {e}")
            raise
    
    
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
                logger.info(f"Read {len(df)} rows from Lakebase")
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
                
                logger.info(f"Table stats: {stats['total_rows']:,} rows")
                return stats
        except Exception as e:
            logger.error(f"Error getting table stats: {e}")
            raise

    def get_foreach_writer(self, creds, database: str = "databricks_postgres", table_name: str = "transaction_features",
                    conflict_columns: List[str] = None,
                    batch_size: int = 10):
        """
        Create a ForeachWriter for per-partition streaming writes
        
        Args:
            table_name: Target table name
            conflict_columns: Columns for ON CONFLICT (default: ["transaction_id"])
            batch_size: Rows to accumulate before writing
            
        Returns:
            LakebaseForeachWriter instance
            
        Example:
            writer = lakebase_client.get_foreach_writer()
            query = df.writeStream.foreach(writer).start()
        """
        if conflict_columns is None:
            conflict_columns = ["transaction_id"]
        
        return LakebaseForeachWriter(
            creds=self.get_credentials(),
            database=database,
            table_name=table_name,
            conflict_columns=conflict_columns,
            batch_size=batch_size
        )        


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




# Add this class to lakebase_client.py

class LakebaseForeachWriter:
    """
    ForeachWriter for per-partition streaming writes to Lakebase
    
    Usage:
        writer = lakebase_client.get_foreach_writer(
            table_name="transaction_features",
            conflict_columns=["transaction_id"]
        )
        query = df.writeStream.foreach(writer).start()
    """
    
    def __init__(self, creds, database, table_name: str, 
                 conflict_columns: List[str], batch_size: int = 100):
        """
        Initialize ForeachWriter
        
        Args:
            creds: lakebase credentials
            database: database name
            table_name: Target table name
            conflict_columns: Columns for ON CONFLICT clause (e.g., ["transaction_id"])
            batch_size: Number of rows to accumulate before writing
        """
        self.creds = creds
        self.database = database
        self.table_name = table_name
        self.conflict_columns = conflict_columns
        self.batch_size = batch_size
        self.max_retries = 3
        
        # Per-partition state
        self.conn = None
        self.cursor = None
        self.current_batch = []
        self.column_names = None
    
    def open(self, partition_id, epoch_id):
        """Open connection for this partition"""
        try:
            logger.info(f"Opening connection for partition {partition_id}, epoch {epoch_id}")
            
            self.conn = psycopg2.connect(
                host=self.creds['host'],
                port=self.creds['port'],
                dbname=self.database,
                user=self.creds['user'],
                password=self.creds['password'],
                connect_timeout=10,
                sslmode='require'
            )
            self.conn.autocommit = False
            self.cursor = self.conn.cursor()
            self.current_batch = []
            return True
        except Exception as e:
            logger.error(f"Error opening connection for partition {partition_id}: {e}")
            return False
    
    def process(self, row):
        """Process a single row"""
        try:
            # Get column names from first row
            if self.column_names is None:
                self.column_names = row.asDict().keys()
            
            # Convert row to values tuple
            values = tuple(row[col] for col in self.column_names)
            self.current_batch.append(values)
            
            # Flush when batch is full
            if len(self.current_batch) >= self.batch_size:
                self._execute_batch()
        except Exception as e:
            logger.error(f"Error processing row: {e}")
            raise
    
    def close(self, error):
        """Close connection and flush remaining batch"""
        try:
            if error is None and self.current_batch:
                self._execute_batch()
            elif error is not None:
                logger.error(f"Error in partition, rolling back: {error}")
                if self.conn:
                    self.conn.rollback()
        except Exception as e:
            logger.error(f"Error during close: {e}")
        finally:
            if self.cursor:
                try:
                    self.cursor.close()
                except Exception:
                    pass
            if self.conn:
                try:
                    self.conn.close()
                except Exception:
                    pass
    
    def _execute_batch(self):
        """Execute the accumulated batch with retry logic"""
        if not self.current_batch:
            return
        
        def _execute():
            start_time = time.time()
            logger.info(f"Writing batch of {len(self.current_batch)} rows...")
            
            # Build upsert SQL
            columns_str = ','.join(self.column_names)
            placeholders = ','.join(['%s'] * len(self.column_names))
            conflict_str = ','.join(self.conflict_columns)
            update_cols = [col for col in self.column_names if col not in self.conflict_columns]
            update_str = ','.join([f"{col}=EXCLUDED.{col}" for col in update_cols])
            
            sql = f"""
                INSERT INTO {self.table_name} ({columns_str})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}
            """
            
            execute_batch(self.cursor, sql, self.current_batch, page_size=self.batch_size)
            self.conn.commit()
            self.current_batch = []
            
            elapsed = time.time() - start_time
            logger.info(f"Batch complete in {elapsed:.2f}s")
        
        # Execute with retry
        self._with_retry(_execute)
    
    def _with_retry(self, fn):
        """Execute function with retry logic"""
        for attempt in range(self.max_retries):
            try:
                fn()
                return
            except (OperationalError, DatabaseError) as e:
                if attempt < self.max_retries - 1:
                    logger.warning(f"Retry {attempt + 1}/{self.max_retries} after error: {e}")
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed after {self.max_retries} retries")
                    raise


# Example usage
if __name__ == "__main__":
    # Example configuration
    client = LakebaseClient(instance_name="neha-lakebase-demo",
        database="databricks_postgres"
    )
    
    # Test connection
    if client.test_connection():
        print("Connected to Lakebase!")
        
        # Create unified feature table (stateless + stateful features)
        # Works for both use cases:
        # - Transaction features (stateless only)
        # - Fraud features (stateless + stateful)
        client.create_feature_table("transaction_features")  # or "fraud_features"
        
        # Get stats
        stats = client.get_table_stats("transaction_features")
        print(f"Table stats: {stats}")
