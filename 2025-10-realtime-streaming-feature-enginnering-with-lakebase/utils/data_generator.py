"""
Synthetic Data Generator for Streaming Feature Engineering
==========================================================

This module generates realistic synthetic transaction data
for testing the streaming feature engineering pipeline.

Author: Databricks
Date: October 2025
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TransactionDataGenerator:
    """Generate realistic synthetic transaction data for streaming"""
    
    def __init__(self, spark_session=None):
        """Initialize the data generator"""
        self.spark = spark_session or SparkSession.getActiveSession()
        self.output_path = "/mnt/lakebase/streaming_input"
        
    def generate_transaction_data(self, num_users=20, num_merchants=50, rows_per_second=10):
        """
        Generate streaming transaction data using rate source
        
        Args:
            num_users: Number of unique users
            num_merchants: Number of unique merchants
            rows_per_second: Number of transactions per second to generate
            
        Returns:
            Streaming DataFrame with transaction data
        """
        logger.info(f"Creating streaming transaction source...")
        logger.info(f"   Rate: {rows_per_second} transactions/second")
        logger.info(f"   Users: {num_users}, Merchants: {num_merchants}")
        
        # Create rate-based streaming source
        rate_stream = self.spark.readStream \
            .format("rate") \
            .option("rowsPerSecond", rows_per_second) \
            .load()
        
        # Transform into transaction data
        streaming_df = rate_stream \
            .withColumn("transaction_id", expr("concat('txn_', lpad(value, 12, '0'))")) \
            .withColumn("user_id_num", expr(f"cast(rand() * {num_users} as int) + 1")) \
            .withColumn("user_id", expr("concat('user_', lpad(cast(user_id_num as string), 6, '0'))")) \
            .withColumn("merchant_id_num", expr(f"cast(rand() * {num_merchants} as int) + 1")) \
            .withColumn("merchant_id", expr("concat('merchant_', lpad(cast(merchant_id_num as string), 6, '0'))")) \
            .withColumn("amount", expr("exp(rand() * 5 + 2) * 10")) \
            .withColumn("currency", 
                       expr("case when rand() < 0.7 then 'USD' " +
                            "when rand() < 0.8 then 'EUR' " +
                            "when rand() < 0.9 then 'GBP' " +
                            "when rand() < 0.95 then 'CAD' " +
                            "else 'AUD' end")) \
            .withColumn("merchant_category",
                       expr("case when rand() < 0.2 then 'restaurant' " +
                            "when rand() < 0.3 then 'gas_station' " +
                            "when rand() < 0.4 then 'grocery' " +
                            "when rand() < 0.5 then 'online_retail' " +
                            "when rand() < 0.6 then 'pharmacy' " +
                            "when rand() < 0.7 then 'department_store' " +
                            "when rand() < 0.8 then 'electronics' " +
                            "when rand() < 0.9 then 'clothing' " +
                            "else 'hotel' end")) \
            .withColumn("payment_method",
                       expr("case when rand() < 0.45 then 'credit_card' " +
                            "when rand() < 0.80 then 'debit_card' " +
                            "when rand() < 0.95 then 'digital_wallet' " +
                            "else 'bank_transfer' end")) \
            .withColumn("ip_address",
                       expr("concat(cast(floor(rand() * 223 + 1) as int), '.', " +
                            "cast(floor(rand() * 255) as int), '.', " +
                            "cast(floor(rand() * 255) as int), '.', " +
                            "cast(floor(rand() * 255) as int))")) \
            .withColumn("device_id",
                       expr("concat('device_', lpad(cast(floor(rand() * (user_id_num * 2)) as string), 8, '0'))")) \
            .withColumn("location_lat", expr("25.0 + rand() * 24.0")) \
            .withColumn("location_lon", expr("-125.0 + rand() * 59.0")) \
            .withColumn("card_type",
                       expr("case when rand() < 0.45 then 'visa' " +
                            "when rand() < 0.80 then 'mastercard' " +
                            "when rand() < 0.92 then 'amex' " +
                            "else 'discover' end")) \
            .drop("value", "user_id_num", "merchant_id_num")
        
        logger.info("Streaming source created successfully")
        return streaming_df


# Example usage
if __name__ == "__main__":
    print("🧪 Testing TransactionDataGenerator...")
    
    generator = TransactionDataGenerator()
    streaming_df = generator.generate_transaction_data(
        num_users=10,
        num_merchants=20,
        rows_per_second=5
    )
    
    print("Streaming DataFrame created")
    print(f"   Is streaming: {streaming_df.isStreaming}")
    streaming_df.printSchema()
    
    # Run a short test query
    print("\nRunning 10-second test query...")
    query = streaming_df.writeStream \
        .format("console") \
        .option("numRows", 3) \
        .start()
    
    time.sleep(10)
    query.stop()
    
    print("\nTest complete!")
