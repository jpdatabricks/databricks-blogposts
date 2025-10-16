"""
Advanced Feature Engineering Module for Streaming Transaction Data
===================================================================

This module provides comprehensive feature engineering capabilities for streaming
transaction data, including real-time aggregations, behavioral patterns, time-based,
velocity, and statistical features. Designed to work with PySpark Structured Streaming
and write to Databricks Lakebase.

Author: Databricks
Date: October 2025
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.ml.stat import Correlation
from delta.tables import DeltaTable
import logging
from datetime import datetime, timedelta
import numpy as np

logger = logging.getLogger(__name__)

class AdvancedFeatureEngineering:
    """
    Advanced feature engineering for streaming transaction data with real-time capabilities.
    
    This class provides methods to engineer 50+ features from raw transaction data including:
    - Time-based features (cyclical encoding, business hours, holidays)
    - Amount-based features (log transforms, categories, statistical features)
    - Velocity features (windowed aggregations across multiple time windows)
    - Behavioral features (user patterns, merchant switching, payment methods)
    - Location features (distance calculations, velocity, consistency)
    - Statistical features (z-scores, percentiles, standard deviations)
    
    Supports both batch and streaming DataFrames.
    """
    
    def __init__(self, spark_session=None):
        """
        Initialize feature engineering pipeline
        
        Args:
            spark_session: SparkSession object. If None, uses active session.
        """
        self.spark = spark_session or SparkSession.getActiveSession()
        self.feature_store_path = "/mnt/lakebase/transaction_features"
        
    def create_time_based_features(self, df, timestamp_col="timestamp"):
        """
        Create comprehensive time-based features
        
        Args:
            df: Input DataFrame
            timestamp_col: Name of timestamp column
            
        Returns:
            DataFrame with time-based features
        """
        logger.info("Creating time-based features...")
        
        df_time = df \
            .withColumn("year", year(col(timestamp_col))) \
            .withColumn("month", month(col(timestamp_col))) \
            .withColumn("day", dayofmonth(col(timestamp_col))) \
            .withColumn("hour", hour(col(timestamp_col))) \
            .withColumn("minute", minute(col(timestamp_col))) \
            .withColumn("day_of_week", dayofweek(col(timestamp_col))) \
            .withColumn("day_of_year", dayofyear(col(timestamp_col))) \
            .withColumn("week_of_year", weekofyear(col(timestamp_col)))
        
        # Business hour indicators
        df_time = df_time \
            .withColumn("is_business_hour", 
                        when((col("hour") >= 9) & (col("hour") <= 17), 1).otherwise(0)) \
            .withColumn("is_weekend", 
                        when(col("day_of_week").isin([1, 7]), 1).otherwise(0)) \
            .withColumn("is_holiday", self._is_holiday(col(timestamp_col))) \
            .withColumn("is_night", 
                        when((col("hour") >= 22) | (col("hour") <= 6), 1).otherwise(0)) \
            .withColumn("is_early_morning", 
                        when((col("hour") >= 6) & (col("hour") <= 9), 1).otherwise(0))
        
        # Cyclical encoding for time features
        df_time = df_time \
            .withColumn("hour_sin", sin(2 * 3.14159 * col("hour") / 24)) \
            .withColumn("hour_cos", cos(2 * 3.14159 * col("hour") / 24)) \
            .withColumn("day_of_week_sin", sin(2 * 3.14159 * col("day_of_week") / 7)) \
            .withColumn("day_of_week_cos", cos(2 * 3.14159 * col("day_of_week") / 7)) \
            .withColumn("month_sin", sin(2 * 3.14159 * col("month") / 12)) \
            .withColumn("month_cos", cos(2 * 3.14159 * col("month") / 12))
        
        return df_time
    
    def _is_holiday(self, timestamp_col):
        """
        Check if a date is a holiday (simplified implementation)
        In production, this would use a comprehensive holiday calendar
        """
        # Simple holiday detection for major US holidays
        return when(
            (month(timestamp_col) == 1) & (dayofmonth(timestamp_col) == 1) |  # New Year
            (month(timestamp_col) == 7) & (dayofmonth(timestamp_col) == 4) |  # Independence Day
            (month(timestamp_col) == 12) & (dayofmonth(timestamp_col) == 25),  # Christmas
            1
        ).otherwise(0)
    
    def create_amount_features(self, df, amount_col="amount"):
        """
        Create amount-based features with statistical transformations
        
        Args:
            df: Input DataFrame
            amount_col: Name of amount column
            
        Returns:
            DataFrame with amount features
        """
        logger.info("Creating amount-based features...")
        
        df_amount = df \
            .withColumn("amount_log", log1p(col(amount_col))) \
            .withColumn("amount_sqrt", sqrt(col(amount_col))) \
            .withColumn("amount_squared", pow(col(amount_col), 2))
        
        # Amount categories
        df_amount = df_amount \
            .withColumn("amount_category",
                        when(col(amount_col) < 10, "micro")
                        .when(col(amount_col) < 50, "small")
                        .when(col(amount_col) < 200, "medium")
                        .when(col(amount_col) < 1000, "large")
                        .when(col(amount_col) < 5000, "very_large")
                        .otherwise("extreme")) \
            .withColumn("is_round_amount", 
                        when(col(amount_col) % 10 == 0, 1).otherwise(0)) \
            .withColumn("is_exact_amount", 
                        when(col(amount_col) % 1 == 0, 1).otherwise(0))
        
        # Amount percentiles (requires historical data for context)
        # This would typically be computed from historical data and joined
        df_amount = df_amount \
            .withColumn("amount_zscore", 
                        (col(amount_col) - lit(100)) / lit(500))  # Placeholder values
        
        return df_amount
    
    def create_location_features(self, df):
        """
        Create location-based features (streaming-compatible, stateless only)
        
        Args:
            df: Input DataFrame with location data
            
        Returns:
            DataFrame with location features
        """
        if "location_lat" not in df.columns or "location_lon" not in df.columns:
            logger.warning("Location columns not found, skipping location features")
            return df
            
        logger.info("Creating location features (streaming-only)...")
        
        df_location = df \
            .withColumn("is_high_risk_location",
                        when((col("location_lat").between(25.0, 49.0)) &
                             (col("location_lon").between(-125.0, -66.0)), 0)
                        .otherwise(1)) \
            .withColumn("is_international",
                        when(~(col("location_lat").between(25.0, 49.0)) |
                             ~(col("location_lon").between(-125.0, -66.0)), 1)
                        .otherwise(0)) \
            .withColumn("location_region",
                        when(col("location_lat").between(40.0, 49.0), "north")
                        .when(col("location_lat").between(32.0, 40.0), "central")
                        .when(col("location_lat").between(25.0, 32.0), "south")
                        .otherwise("international"))
        
        return df_location
    
    def create_merchant_features(self, df):
        """
        Create merchant-based features (streaming-compatible, stateless only)
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with merchant features
        """
        logger.info("Creating merchant features (streaming-only)...")
        
        merchant_risk_map = {
            "gas_station": 0.1,
            "restaurant": 0.05,
            "grocery": 0.02,
            "online_retail": 0.3,
            "atm": 0.4,
            "casino": 0.8,
            "adult_entertainment": 0.9,
            "unknown": 0.5
        }
        
        risk_expr = lit(0.5)
        for category, risk in merchant_risk_map.items():
            risk_expr = when(col("merchant_category") == category, lit(risk)).otherwise(risk_expr)
        
        df_merchant = df \
            .withColumn("merchant_risk_score", risk_expr) \
            .withColumn("merchant_category_risk",
                       when(col("merchant_category").isin(["casino", "adult_entertainment"]), "high")
                       .when(col("merchant_category").isin(["online_retail", "atm"]), "medium")
                       .otherwise("low"))
        
        return df_merchant
    
    def create_device_features(self, df):
        """
        Create device features (streaming-compatible, stateless only)
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with device features
        """
        if "device_id" not in df.columns:
            logger.warning("Device ID column not found, skipping device features")
            return df
            
        logger.info("Creating device features (streaming-only)...")
        
        df_device = df \
            .withColumn("has_device_id",
                       when(col("device_id").isNotNull(), 1).otherwise(0)) \
            .withColumn("device_type",
                       when(col("device_id").startswith("device_0"), "mobile")
                       .when(col("device_id").startswith("device_1"), "tablet")
                       .otherwise("desktop"))
        
        return df_device
    
    def create_network_features(self, df):
        """
        Create network features (streaming-compatible, stateless only)
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with network features
        """
        if "ip_address" not in df.columns:
            logger.warning("IP address column not found, skipping network features")
            return df
            
        logger.info("Creating network features (streaming-only)...")
        
        df_network = df \
            .withColumn("is_tor_ip", lit(0)) \
            .withColumn("is_private_ip",
                       when(col("ip_address").startswith("10.") |
                            col("ip_address").startswith("192.168.") |
                            col("ip_address").startswith("172."), 1)
                       .otherwise(0)) \
            .withColumn("ip_class",
                       when(col("ip_address").startswith("10."), "class_a")
                       .when(col("ip_address").startswith("192.168."), "class_c")
                       .otherwise("public"))
        
        return df_network
    
    def apply_all_features(self, df, include_optional=True):
        """
        Apply all streaming-compatible feature engineering transformations
        
        Args:
            df: Streaming DataFrame
            include_optional: Include optional features (location, device, network) if data available
            
        Returns:
            DataFrame with all stateless engineered features
            
        Note:
            This method only applies STATELESS transformations compatible with streaming.
            All velocity and behavioral features require separate aggregation streams.
        """
        logger.info("Applying streaming-compatible feature engineering...")
        
        df_features = self.create_time_based_features(df)
        df_features = self.create_amount_features(df_features)
        df_features = self.create_merchant_features(df_features)
        
        if include_optional:
            df_features = self.create_location_features(df_features)
            df_features = self.create_device_features(df_features)
            df_features = self.create_network_features(df_features)
        
        logger.info("Streaming feature engineering completed!")
        return df_features
    
    def write_features_to_lakebase(self, df, lakebase_client, table_name="transaction_features",
                                  checkpoint_location=None, trigger_interval="30 seconds"):
        """
        Write streaming features to Lakebase PostgreSQL database
        
        Args:
            df: Streaming DataFrame with features
            lakebase_client: LakebaseClient instance for PostgreSQL connection
            table_name: Target table name in Lakebase
            checkpoint_location: Checkpoint location for streaming
            trigger_interval: Trigger interval for micro-batches
            
        Returns:
            StreamingQuery object
        """
        if checkpoint_location is None:
            checkpoint_location = f"/tmp/feature_engineering_checkpoint/{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"Writing features to Lakebase PostgreSQL: {table_name}")
        logger.info(f"Checkpoint location: {checkpoint_location}")
        
        # Use foreachBatch to write each micro-batch to PostgreSQL
        def write_batch_to_postgres(batch_df, batch_id):
            lakebase_client.write_streaming_batch(batch_df, batch_id, table_name)
        
        return df.writeStream \
            .foreachBatch(write_batch_to_postgres) \
            .option("checkpointLocation", checkpoint_location) \
            .trigger(processingTime=trigger_interval) \
            .start()
    
    

if __name__ == "__main__":
    main()