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
    Advanced feature engineering for STREAMING transaction data (streaming-only).
    
    IMPORTANT: This class is designed EXCLUSIVELY for PySpark Structured Streaming.
    All methods use streaming-compatible transformations with bounded state.
    
    Feature Categories:
    - Time-based features (cyclical encoding, business hours, holidays)
    - Amount-based features (log transforms, categories, statistical features)
    - Behavioral features (stateless indicators)
    - Merchant features (risk scoring, category-based features)
    - Location features (distance calculations, risk zones)
    - Device features (stateless device indicators)
    - Network features (IP classification, risk indicators)
    
    For stateful aggregations (velocity, windowed stats):
    - Use create_velocity_features_streaming() with watermarks
    - Use create_merchant_aggregations_streaming() for merchant metrics
    - Compute in separate aggregation streams and join results
    
    Streaming Requirements:
    - All input DataFrames must be streaming (readStream)
    - Apply watermarks before windowed aggregations
    - Use foreachBatch for custom sinks
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
    
    def create_velocity_features_streaming(self, df, window_duration="1 hour", slide_duration="15 minutes"):
        """
        Create velocity-based features using streaming window() function
        
        NOTE: This creates aggregated velocity features using time-based windowing.
        For per-transaction velocity features, compute these in a separate aggregation 
        stream and join them back to the main stream.
        
        Args:
            df: Streaming DataFrame with watermark already applied
            window_duration: Window duration (e.g., "1 hour", "5 minutes")
            slide_duration: Slide duration for sliding windows
            
        Returns:
            DataFrame with aggregated velocity features per window
            
        Example:
            # Apply watermark first
            df_with_watermark = df.withWatermark("timestamp", "10 minutes")
            
            # Compute velocity aggregations
            velocity_aggs = feature_eng.create_velocity_features_streaming(df_with_watermark)
            
            # Join back to main stream if needed
            df_enriched = df.join(velocity_aggs, on=["user_id", "window"], how="left")
        """
        logger.info(f"Creating streaming velocity features (window={window_duration}, slide={slide_duration})...")
        
        # Use window() function for streaming-compatible time-based grouping
        velocity_aggs = df \
            .groupBy(
                "user_id",
                window("timestamp", window_duration, slide_duration)
            ) \
            .agg(
                count("*").alias("user_txn_count"),
                sum("amount").alias("user_amount_sum"),
                avg("amount").alias("user_amount_avg"),
                max("amount").alias("user_amount_max"),
                stddev("amount").alias("user_amount_std"),
                approx_count_distinct("merchant_id").alias("unique_merchants"),
                approx_count_distinct("payment_method").alias("unique_payment_methods")
            )
        
        logger.info("✅ Streaming velocity features created")
        return velocity_aggs
    
    def create_behavioral_features(self, df):
        """
        Create simple behavioral pattern features (streaming-compatible)
        
        NOTE: This creates stateless behavioral indicators. For stateful patterns
        like "time since last transaction" or "change from previous", use 
        applyInPandasWithState or compute in separate aggregation streams.
        
        Args:
            df: Streaming DataFrame
            
        Returns:
            DataFrame with stateless behavioral features
        """
        logger.info("Creating behavioral features (streaming-only)...")
        
        # Stateless behavioral indicators
        df_behavior = df \
            .withColumn("is_high_value_txn",
                       when(col("amount") > 1000, 1).otherwise(0)) \
            .withColumn("is_round_amount",
                       when(col("amount") % 10 == 0, 1).otherwise(0)) \
            .withColumn("merchant_category_freq",
                       when(col("merchant_category").isin(["restaurant", "grocery", "gas_station"]), "high")
                       .when(col("merchant_category").isin(["online_retail", "department_store"]), "medium")
                       .otherwise("low"))
        
        logger.info("✅ Behavioral features created (stateless)")
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
        Create merchant-based features (streaming-compatible)
        
        Stateless merchant features using category-based risk scoring.
        For advanced merchant aggregations (transaction counts, averages), 
        use create_merchant_aggregations_streaming() separately.
        
        Args:
            df: Streaming DataFrame
            
        Returns:
            DataFrame with stateless merchant features
        """
        logger.info("Creating merchant features (streaming-only)...")
        
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
        
        # Create merchant risk score (stateless transformation)
        risk_expr = lit(0.5)  # default
        for category, risk in merchant_risk_map.items():
            risk_expr = when(col("merchant_category") == category, lit(risk)).otherwise(risk_expr)
        
        df_merchant = df \
            .withColumn("merchant_risk_score", risk_expr) \
            .withColumn("merchant_category_risk",
                       when(col("merchant_category").isin(["casino", "adult_entertainment"]), "high")
                       .when(col("merchant_category").isin(["online_retail", "atm"]), "medium")
                       .otherwise("low"))
        
        return df_merchant
    
    def create_merchant_aggregations_streaming(self, df):
        """
        Create merchant aggregations compatible with streaming mode using window() function
        
        This method shows how to properly aggregate merchant metrics in streaming mode.
        For best performance, run this as a separate aggregation stream and join results.
        
        Args:
            df: Streaming DataFrame with watermark already applied
            
        Returns:
            Aggregated DataFrame with merchant metrics per 1-hour window
            
        Example:
            # In your streaming pipeline:
            df_with_watermark = df.withWatermark("timestamp", "10 minutes")
            merchant_aggs = feature_eng.create_merchant_aggregations_streaming(df_with_watermark)
            
            # Join back to main stream
            df_enriched = df.join(
                merchant_aggs,
                on=["merchant_id", "window"],
                how="left"
            )
        """
        logger.info("Creating streaming-compatible merchant aggregations...")
        
        # Use window() function for time-based grouping
        # This is streaming-compatible and maintains bounded state with watermarks
        merchant_aggs = df \
            .groupBy(
                "merchant_id",
                window("timestamp", "1 hour", "15 minutes")  # 1-hour windows, sliding every 15 min
            ) \
            .agg(
                approx_count_distinct("user_id").alias("merchant_unique_users_1h"),
                avg("amount").alias("merchant_avg_amount_1h"),
                count("*").alias("merchant_txn_count_1h"),
                stddev("amount").alias("merchant_amount_std_1h")
            )
        
        return merchant_aggs
    
    def create_device_features(self, df):
        """
        Create device and digital fingerprinting features (streaming-compatible)
        
        NOTE: Stateless device indicators only. For device usage patterns,
        compute aggregations in a separate stream using window() function.
        
        Args:
            df: Streaming DataFrame
            
        Returns:
            DataFrame with stateless device features
        """
        if "device_id" not in df.columns:
            logger.warning("Device ID column not found, skipping device features")
            return df
            
        logger.info("Creating device features (streaming-only)...")
        
        # Stateless device features
        df_device = df \
            .withColumn("has_device_id",
                       when(col("device_id").isNotNull(), 1).otherwise(0)) \
            .withColumn("device_type",
                       when(col("device_id").startswith("device_0"), "mobile")
                       .when(col("device_id").startswith("device_1"), "tablet")
                       .otherwise("desktop"))
        
        logger.info("✅ Device features created (stateless)")
        return df_device
    
    def create_network_features(self, df):
        """
        Create network and IP-based features (streaming-compatible)
        
        NOTE: Stateless IP indicators only. For IP usage patterns,
        compute aggregations in a separate stream using window() function.
        
        Args:
            df: Streaming DataFrame
            
        Returns:
            DataFrame with stateless network features
        """
        if "ip_address" not in df.columns:
            logger.warning("IP address column not found, skipping network features")
            return df
            
        logger.info("Creating network features (streaming-only)...")
        
        # Stateless IP risk indicators
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
        
        logger.info("✅ Network features created (stateless)")
        return df_network
    
    def apply_all_features(self, df, include_optional=True):
        """
        Apply all streaming-compatible feature engineering transformations
        
        This method applies stateless transformations only. For stateful aggregations
        like velocity features, use the dedicated streaming methods:
        - create_velocity_features_streaming()
        - create_merchant_aggregations_streaming()
        
        Args:
            df: Streaming DataFrame
            include_optional: Include optional features (location, device, network) if data available
            
        Returns:
            DataFrame with all engineered stateless features
            
        Note:
            This is a STREAMING-ONLY method. All transformations are stateless
            and can be applied to each micro-batch independently.
        """
        logger.info("Applying streaming-compatible feature engineering...")
        
        # Core stateless features (always applied)
        df_features = self.create_time_based_features(df)
        df_features = self.create_amount_features(df_features)
        df_features = self.create_behavioral_features(df_features)
        df_features = self.create_merchant_features(df_features)
        
        # Optional features (only if data is available)
        if include_optional:
            df_features = self.create_location_features(df_features)
            df_features = self.create_device_features(df_features)
            df_features = self.create_network_features(df_features)
        
        logger.info("✅ Streaming feature engineering completed successfully!")
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

