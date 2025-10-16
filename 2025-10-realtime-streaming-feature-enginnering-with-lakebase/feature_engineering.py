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
    
    def create_velocity_features(self, df, timestamp_col="timestamp"):
        """
        Create velocity-based features using time windows
        
        Args:
            df: Input DataFrame with watermark
            timestamp_col: Name of timestamp column
            
        Returns:
            DataFrame with velocity features
        """
        logger.info("Creating velocity features...")
        
        # Define time windows (in seconds)
        windows = {
            "5m": 300,
            "15m": 900,
            "1h": 3600,
            "6h": 21600,
            "24h": 86400
        }
        
        df_velocity = df
        
        for window_name, window_seconds in windows.items():
            # User-based velocity features
            user_window = Window.partitionBy("user_id") \
                               .orderBy(col(timestamp_col).cast("long")) \
                               .rangeBetween(-window_seconds, 0)
            
            df_velocity = df_velocity \
                .withColumn(f"user_txn_count_{window_name}",
                           count("*").over(user_window)) \
                .withColumn(f"user_amount_sum_{window_name}",
                           sum("amount").over(user_window)) \
                .withColumn(f"user_amount_avg_{window_name}",
                           avg("amount").over(user_window)) \
                .withColumn(f"user_amount_max_{window_name}",
                           max("amount").over(user_window)) \
                .withColumn(f"user_amount_std_{window_name}",
                           stddev("amount").over(user_window))
            
            # Merchant-based velocity features
            merchant_window = Window.partitionBy("merchant_id") \
                                   .orderBy(col(timestamp_col).cast("long")) \
                                   .rangeBetween(-window_seconds, 0)
            
            df_velocity = df_velocity \
                .withColumn(f"merchant_txn_count_{window_name}",
                           count("*").over(merchant_window)) \
                .withColumn(f"merchant_amount_sum_{window_name}",
                           sum("amount").over(merchant_window))
            
            # Device-based velocity features
            if "device_id" in df.columns:
                device_window = Window.partitionBy("device_id") \
                                     .orderBy(col(timestamp_col).cast("long")) \
                                     .rangeBetween(-window_seconds, 0)
                
                df_velocity = df_velocity \
                    .withColumn(f"device_txn_count_{window_name}",
                               count("*").over(device_window))
            
            # IP-based velocity features
            if "ip_address" in df.columns:
                ip_window = Window.partitionBy("ip_address") \
                                 .orderBy(col(timestamp_col).cast("long")) \
                                 .rangeBetween(-window_seconds, 0)
                
                df_velocity = df_velocity \
                    .withColumn(f"ip_txn_count_{window_name}",
                               count("*").over(ip_window))
        
        return df_velocity
    
    def create_behavioral_features(self, df, timestamp_col="timestamp"):
        """
        Create behavioral pattern features
        
        Args:
            df: Input DataFrame
            timestamp_col: Name of timestamp column
            
        Returns:
            DataFrame with behavioral features
        """
        logger.info("Creating behavioral features...")
        
        # User behavior patterns
        user_window = Window.partitionBy("user_id").orderBy(timestamp_col)
        
        df_behavior = df \
            .withColumn("time_since_last_txn",
                       col(timestamp_col).cast("long") - 
                       lag(col(timestamp_col).cast("long")).over(user_window)) \
            .withColumn("amount_vs_prev",
                       col("amount") - lag("amount").over(user_window)) \
            .withColumn("merchant_change",
                       when(col("merchant_id") != lag("merchant_id").over(user_window), 1)
                       .otherwise(0)) \
            .withColumn("payment_method_change",
                       when(col("payment_method") != lag("payment_method").over(user_window), 1)
                       .otherwise(0))
        
        # Merchant switching patterns
        df_behavior = df_behavior \
            .withColumn("unique_merchants_1h",
                       approx_count_distinct("merchant_id").over(
                           Window.partitionBy("user_id")
                           .orderBy(col(timestamp_col).cast("long"))
                           .rangeBetween(-3600, 0)
                       )) \
            .withColumn("unique_payment_methods_1h",
                       approx_count_distinct("payment_method").over(
                           Window.partitionBy("user_id")
                           .orderBy(col(timestamp_col).cast("long"))
                           .rangeBetween(-3600, 0)
                       ))
        
        return df_behavior
    
    def create_location_features(self, df):
        """
        Create location-based features
        
        Args:
            df: Input DataFrame with location data
            
        Returns:
            DataFrame with location features
        """
        if "location_lat" not in df.columns or "location_lon" not in df.columns:
            logger.warning("Location columns not found, skipping location features")
            return df
            
        logger.info("Creating location features...")
        
        user_window = Window.partitionBy("user_id").orderBy("timestamp")
        
        df_location = df \
            .withColumn("prev_lat", lag("location_lat").over(user_window)) \
            .withColumn("prev_lon", lag("location_lon").over(user_window))
        
        # Calculate distance from previous transaction
        df_location = df_location \
            .withColumn("distance_from_prev",
                       when((col("prev_lat").isNotNull()) & (col("prev_lon").isNotNull()),
                            self._haversine_distance("location_lat", "location_lon",
                                                   "prev_lat", "prev_lon"))
                       .otherwise(0))
        
        # Velocity (distance/time)
        # df_location = df_location \
        #     .withColumn("location_velocity",
        #                when((col("time_since_last_txn") > 0) & (col("distance_from_prev") > 0),
        #                     col("distance_from_prev") / (col("time_since_last_txn") / 3600))
        #                .otherwise(0))
        
        # Location risk zones (simplified)
        df_location = df_location \
            .withColumn("is_high_risk_location",
                       when((col("location_lat").between(25.0, 49.0)) &
                            (col("location_lon").between(-125.0, -66.0)), 0)  # US mainland
                       .otherwise(1))  # International or unusual locations
        
        # Location consistency
        df_location = df_location \
            .withColumn("location_consistency_score",
                       when(col("distance_from_prev") < 10, 1.0)  # Same city
                       .when(col("distance_from_prev") < 100, 0.8)  # Same state
                       .when(col("distance_from_prev") < 1000, 0.5)  # Same country
                       .otherwise(0.1))  # International
        
        return df_location
    
    def _haversine_distance(self, lat1, lon1, lat2, lon2):
        """Calculate Haversine distance between two points"""
        return acos(
            sin(radians(col(lat1))) * sin(radians(col(lat2))) +
            cos(radians(col(lat1))) * cos(radians(col(lat2))) *
            cos(radians(col(lon1)) - radians(col(lon2)))
        ) * 6371  # Earth's radius in km
    
    def create_merchant_features(self, df):
        """
        Create merchant-based features
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with merchant features
        """
        logger.info("Creating merchant features...")
        
        # Merchant risk scores (would be computed from historical data)
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
        
        # Create merchant risk score
        risk_expr = lit(0.5)  # default
        for category, risk in merchant_risk_map.items():
            risk_expr = when(col("merchant_category") == category, lit(risk)).otherwise(risk_expr)
        
        df_merchant = df.withColumn("merchant_risk_score", risk_expr)
        
        # Merchant transaction patterns
        merchant_window_1h = Window.partitionBy("merchant_id") \
                                  .orderBy(col("timestamp").cast("long")) \
                                  .rangeBetween(-3600, 0)
        
        # df_merchant = df_merchant \
            .withColumn("merchant_unique_users_1h",
                       approx_count_distinct("user_id").over(merchant_window_1h)) \
            .withColumn("merchant_avg_amount_1h",
                       avg("amount").over(merchant_window_1h))
        
        # User-merchant interaction history
        user_merchant_window = Window.partitionBy("user_id", "merchant_id").orderBy("timestamp")
        
        # df_merchant = df_merchant \
            .withColumn("user_merchant_txn_count",
                       count("*").over(user_merchant_window)) \
            .withColumn("is_new_merchant",
                       when(col("user_merchant_txn_count") == 1, 1).otherwise(0))
        
        return df_merchant
    
    def create_device_features(self, df):
        """
        Create device and digital fingerprinting features
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with device features
        """
        if "device_id" not in df.columns:
            logger.warning("Device ID column not found, skipping device features")
            return df
            
        logger.info("Creating device features...")
        
        # Device usage patterns
        device_window_1h = Window.partitionBy("device_id") \
                                .orderBy(col("timestamp").cast("long")) \
                                .rangeBetween(-3600, 0)
        
        df_device = df \
            .withColumn("device_unique_users_1h",
                       approx_count_distinct("user_id").over(device_window_1h)) \
            .withColumn("device_unique_merchants_1h",
                       approx_count_distinct("merchant_id").over(device_window_1h))
        
        # User-device relationship
        user_device_window = Window.partitionBy("user_id", "device_id").orderBy("timestamp")
        
        df_device = df_device \
            .withColumn("user_device_txn_count",
                       count("*").over(user_device_window)) \
            .withColumn("is_new_device",
                       when(col("user_device_txn_count") == 1, 1).otherwise(0))
        
        # Device sharing indicators
        df_device = df_device \
            .withColumn("device_sharing_risk",
                       when(col("device_unique_users_1h") > 3, 1).otherwise(0))
        
        return df_device
    
    def create_network_features(self, df):
        """
        Create network and IP-based features
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with network features
        """
        if "ip_address" not in df.columns:
            logger.warning("IP address column not found, skipping network features")
            return df
            
        logger.info("Creating network features...")
        
        # IP usage patterns
        ip_window_1h = Window.partitionBy("ip_address") \
                            .orderBy(col("timestamp").cast("long")) \
                            .rangeBetween(-3600, 0)
        
        df_network = df \
            .withColumn("ip_unique_users_1h",
                       approx_count_distinct("user_id").over(ip_window_1h)) \
            .withColumn("ip_unique_devices_1h",
                       approx_count_distinct("device_id").over(ip_window_1h))
        
        # IP risk indicators
        df_network = df_network \
            .withColumn("ip_sharing_risk",
                       when(col("ip_unique_users_1h") > 5, 1).otherwise(0)) \
            .withColumn("is_tor_ip", lit(0))  # Would implement actual Tor detection
        
        # User-IP relationship
        user_ip_window = Window.partitionBy("user_id", "ip_address").orderBy("timestamp")
        
        df_network = df_network \
            .withColumn("user_ip_txn_count",
                       count("*").over(user_ip_window)) \
            .withColumn("is_new_ip",
                       when(col("user_ip_txn_count") == 1, 1).otherwise(0))
        
        return df_network
    
    def create_statistical_features(self, df):
        """
        Create statistical features based on user behavior
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with statistical features
        """
        logger.info("Creating statistical features...")
        
        # User statistical patterns over 24h window
        user_window_24h = Window.partitionBy("user_id") \
                               .orderBy(col("timestamp").cast("long")) \
                               .rangeBetween(-86400, 0)
        
        df_stats = df \
            .withColumn("user_amount_std_24h",
                       stddev("amount").over(user_window_24h)) \
            .withColumn("user_amount_median_24h",
                       expr("percentile_approx(amount, 0.5)").over(user_window_24h)) \
            .withColumn("user_amount_p95_24h",
                       expr("percentile_approx(amount, 0.95)").over(user_window_24h))
        
        # Z-score for current transaction
        # df_stats = df_stats \
        #     .withColumn("amount_zscore_24h",
        #                when(col("user_amount_std_24h") > 0,
        #                     (col("amount") - col("user_amount_avg_24h")) / col("user_amount_std_24h"))
        #                .otherwise(0))
        
        return df_stats
    
    def apply_all_features(self, df, include_optional=True):
        """
        Apply all feature engineering transformations
        
        Args:
            df: Input DataFrame (can be batch or streaming)
            include_optional: Include optional features (location, device, network) if data available
            
        Returns:
            DataFrame with all engineered features
            
        Note:
            For streaming DataFrames, ensure watermark is set on timestamp column before calling this method
            if using windowed aggregations.
        """
        logger.info("Applying comprehensive feature engineering...")
        
        # Core features (always applied)
        #df_features = self.create_time_based_features(df)
        df_features = self.create_amount_features(df)
        #df_features = self.create_velocity_features(df_features)
        #df_features = self.create_behavioral_features(df_features)
        df_features = self.create_merchant_features(df_features)
        print("************* DONE ADDING FEATURES")
        
        # Optional features (only if data is available)
        if include_optional:            
            x = 1
            #df_features = self.create_location_features(df_features)
            #df_features = self.create_device_features(df_features)
            #df_features = self.create_network_features(df_features)
            #df_features = self.create_statistical_features(df_features)

            #df_features = self.create_location_features(df_features)
            #df_features = self.create_device_features(df_features)
            #df_features = self.create_network_features(df_features)
            #df_features = self.create_statistical_features(df_features)
        
        logger.info("Feature engineering completed successfully!")
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

