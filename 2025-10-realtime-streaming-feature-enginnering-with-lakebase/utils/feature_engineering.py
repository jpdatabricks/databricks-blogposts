"""
Advanced Feature Engineering Module for Streaming Transaction Data
===================================================================

This module provides comprehensive feature engineering capabilities for
real-time transaction data, including both stateless and stateful transformations.

**Components:**

1. **AdvancedFeatureEngineering (Class)**: Stateless feature transformations
   - Time-based features (cyclical encoding, business hours, holidays)
   - Amount-based features (log transforms, categories, z-scores)
   - Merchant features (risk scores based on category)
   - Location features (risk indicators, region classification)
   - Device features (device type detection)
   - Network features (IP classification, private/public)

2. **FraudDetectionFeaturesProcessor (Class)**: Stateful fraud detection processor
   - Transaction velocity analysis (time windows)
   - IP address change tracking
   - Geographic anomaly detection (impossible travel)
   - Amount-based anomaly detection (z-scores, ratios)
   - Composite fraud scoring (0-100 scale)
   - Uses transformWithStateInPandas (Spark 4.0+)

3. **Helper Functions**:
   - calculate_haversine_distance(): Geographic distance calculation

**STREAMING-ONLY DESIGN**: This module does NOT support batch processing.
All features are designed to work with PySpark Structured Streaming and
Databricks Lakebase PostgreSQL.

**Usage:**
```python
from utils.feature_engineering import AdvancedFeatureEngineering, FraudDetectionFeaturesProcessor

# Stateless features
feature_engineer = AdvancedFeatureEngineering(spark)
df_with_features = feature_engineer.apply_all_features(df_streaming)

# Stateful fraud detection
df_with_fraud = df_streaming \\
    .withWatermark("timestamp", "10 minutes") \\
    .groupBy("user_id") \\
    .transformWithStateInPandas(
        statefulProcessor=FraudDetectionFeaturesProcessor(),
        outputStructType=fraud_output_schema,
        outputMode="Append",
        timeMode="None"
    )
```

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
    Advanced feature engineering for streaming transaction data.
    
    **STREAMING-ONLY DESIGN**: All methods produce stateless transformations
    compatible with PySpark Structured Streaming.
    
    This class provides methods to engineer features from raw transaction data:
    - Time-based features (cyclical encoding, business hours, holidays, year/month/day)
    - Amount-based features (log transforms, categories, z-scores)
    - Merchant features (risk scores based on category)
    - Location features (risk indicators, region classification - optional)
    - Device features (device type detection - optional)
    - Network features (IP classification, private/public - optional)
    
    **Note**: For stateful features (velocity, behavioral patterns), use separate
    streaming aggregation pipelines with windowed groupBy operations.
    """
    
    def __init__(self, spark_session=None):
        """
        Initialize feature engineering pipeline
        
        Args:
            spark_session: SparkSession object. If None, uses active session.
        """
        self.spark = spark_session or SparkSession.getActiveSession()
        
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
    
    


# =============================================================================
# STATEFUL FRAUD DETECTION PROCESSOR
# =============================================================================

def calculate_haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate distance between two geographic points in kilometers using Haversine formula.
    
    Args:
        lat1, lon1: Latitude and longitude of first point
        lat2, lon2: Latitude and longitude of second point
    
    Returns:
        Distance in kilometers, or None if any coordinate is missing
    """
    import pandas as pd
    import numpy as np
    
    if pd.isna(lat1) or pd.isna(lon1) or pd.isna(lat2) or pd.isna(lon2):
        return None
    
    R = 6371.0  # Earth radius in kilometers
    
    # Convert to radians
    lat1_rad = np.radians(lat1)
    lon1_rad = np.radians(lon1)
    lat2_rad = np.radians(lat2)
    lon2_rad = np.radians(lon2)
    
    # Haversine formula
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = np.sin(dlat/2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon/2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    
    return R * c


class FraudDetectionFeaturesProcessor:
    """
    StatefulProcessor for real-time fraud detection using transformWithStateInPandas.
    
    This processor maintains consolidated state per user to detect fraud patterns including:
    - Transaction velocity (time windows)
    - IP address changes
    - Geographic anomalies (impossible travel)
    - Amount-based anomalies
    
    **Requirements:**
    - Apache Spark 4.0+ (for transformWithStateInPandas support)
    - PySpark Structured Streaming
    
    **Usage:**
    ```python
    from pyspark.sql.streaming import StatefulProcessor
    from utils.feature_engineering import FraudDetectionFeaturesProcessor
    
    # Apply to streaming DataFrame
    df_with_fraud_features = df_transactions \\
        .withWatermark("timestamp", "10 minutes") \\
        .groupBy("user_id") \\
        .transformWithStateInPandas(
            statefulProcessor=FraudDetectionFeaturesProcessor(),
            outputStructType=output_schema,
            outputMode="Append",
            timeMode="None"
        )
    ```
    
    **State Management:**
    Uses a single consolidated ValueState object containing:
    - Transaction count
    - Last transaction details (timestamp, IP, location)
    - IP change count
    - Amount statistics (total, avg, max)
    - Recent transaction history (bounded to 50 transactions)
    
    **Fraud Score Calculation (0-100 points):**
    - Rapid transactions (5+ in 10 min): +20 points
    - Impossible travel (>800 km/h): +30 points
    - Amount anomaly (z-score > 3): +25 points
    - Frequent IP changes (5+ total): +15 points
    - High velocity (10+ in 1 hour): +10 points
    
    **Fraud Prediction:** Score >= 50 triggers fraud flag
    """
    
    def init(self, handle) -> None:
        """
        Initialize the stateful processor with a single consolidated state variable.
        
        Consolidated state includes:
        - Transaction count
        - Last transaction details (timestamp, IP, location)
        - IP change count
        - Amount statistics (total, avg, max)
        - Recent transaction times (up to 50)
        - Recent transaction amounts (up to 50)
        
        Args:
            handle: StatefulProcessorHandle for state management
        """
        from pyspark.sql.streaming.state import TTLConfig
        
        self.handle = handle
        
        # Define comprehensive state schema - consolidates ALL state into one object
        state_schema = StructType([
            # Transaction count
            StructField("transaction_count", IntegerType(), False),
            
            # Last transaction details
            StructField("last_timestamp", TimestampType(), True),
            StructField("last_ip_address", StringType(), True),
            StructField("last_latitude", DoubleType(), True),
            StructField("last_longitude", DoubleType(), True),
            
            # IP change tracking
            StructField("ip_change_count", IntegerType(), False),
            
            # Amount statistics
            StructField("total_amount", DoubleType(), False),
            StructField("avg_amount", DoubleType(), False),
            StructField("max_amount", DoubleType(), False),
            
            # Recent transaction history (bounded to 50 each)
            StructField("recent_timestamps", ArrayType(TimestampType()), False),
            StructField("recent_amounts", ArrayType(DoubleType()), False)
        ])
        
        # Initialize SINGLE consolidated state variable with TTL (1 hour of inactivity)
        ttl_config = TTLConfig(ttl_duration=timedelta(hours=1))
        
        self.user_state = handle.getValueState(
            "user_fraud_state",  # Single state variable name
            state_schema,
            ttl_config
        )
    
    def handleInputRows(self, key, rows, timer_values):
        """
        Process input rows for a given user and emit fraud features.
        
        Args:
            key: user_id
            rows: Iterator of Pandas DataFrames containing transactions for this user
            timer_values: Timer values (not used in this implementation)
        
        Yields:
            pd.DataFrame: Enriched transactions with fraud features
        """
        import pandas as pd
        import numpy as np
        
        user_id = key
        
        # Process each micro-batch
        for pdf in rows:
            if pdf.empty:
                continue
            
            # Sort by timestamp
            pdf = pdf.sort_values('timestamp')
            
            # Retrieve existing state (single consolidated object)
            if self.user_state.exists():
                state = self.user_state.get()
                prev_count = state[0]  # transaction_count
                prev_last_time = state[1]  # last_timestamp
                prev_ip = state[2]  # last_ip_address
                prev_lat = state[3]  # last_latitude
                prev_lon = state[4]  # last_longitude
                prev_ip_changes = state[5]  # ip_change_count
                prev_total_amount = state[6]  # total_amount
                prev_avg_amount = state[7]  # avg_amount
                prev_max_amount = state[8]  # max_amount
                prev_times = list(state[9]) if state[9] else []  # recent_timestamps
                prev_amounts = list(state[10]) if state[10] else []  # recent_amounts
            else:
                # Initialize state for new user
                prev_count = 0
                prev_last_time = None
                prev_ip = None
                prev_lat = None
                prev_lon = None
                prev_ip_changes = 0
                prev_total_amount = 0.0
                prev_avg_amount = 0.0
                prev_max_amount = 0.0
                prev_times = []
                prev_amounts = []
            
            # Process each transaction
            results = []
            
            for idx, row in pdf.iterrows():
                current_time = row['timestamp']
                current_ip = row['ip_address']
                current_lat = row['latitude']
                current_lon = row['longitude']
                current_amount = row['amount']
                
                # Update transaction count
                prev_count += 1
                
                # Calculate time-based features
                if prev_last_time is not None:
                    time_diff = (current_time - prev_last_time).total_seconds()
                else:
                    time_diff = None
                
                # IP change detection
                ip_changed = 0
                if prev_ip is not None and current_ip != prev_ip:
                    ip_changed = 1
                    prev_ip_changes += 1
                
                # Geographic distance calculation
                distance_km = None
                velocity_kmh = None
                if prev_lat is not None and prev_lon is not None:
                    distance_km = calculate_haversine_distance(
                        prev_lat, prev_lon, current_lat, current_lon
                    )
                    if distance_km is not None and time_diff is not None and time_diff > 0:
                        velocity_kmh = (distance_km / time_diff) * 3600
                
                # Amount-based features
                prev_total_amount += current_amount
                prev_avg_amount = prev_total_amount / prev_count
                prev_max_amount = max(prev_max_amount, current_amount)
                
                amount_vs_avg_ratio = current_amount / prev_avg_amount if prev_avg_amount > 0 else 1.0
                amount_vs_max_ratio = current_amount / prev_max_amount if prev_max_amount > 0 else 1.0
                
                # Z-score calculation
                amount_zscore = None
                if len(prev_amounts) >= 3:
                    amounts_std = np.std(prev_amounts)
                    if amounts_std > 0:
                        amount_zscore = (current_amount - prev_avg_amount) / amounts_std
                
                # Update recent transactions (bounded to 50)
                prev_times.append(current_time)
                prev_amounts.append(current_amount)
                if len(prev_times) > 50:
                    prev_times = prev_times[-50:]
                    prev_amounts = prev_amounts[-50:]
                
                # Count transactions in time windows
                one_hour_ago = current_time - timedelta(hours=1)
                ten_min_ago = current_time - timedelta(minutes=10)
                
                trans_last_hour = sum(1 for t in prev_times if t >= one_hour_ago)
                trans_last_10min = sum(1 for t in prev_times if t >= ten_min_ago)
                
                # Fraud indicators
                is_rapid = 1 if trans_last_10min >= 5 else 0
                is_impossible_travel = 1 if velocity_kmh is not None and velocity_kmh > 800 else 0
                is_amount_anomaly = 1 if amount_zscore is not None and abs(amount_zscore) > 3 else 0
                
                # Calculate fraud score (0-100)
                fraud_score = 0.0
                if is_rapid:
                    fraud_score += 20
                if is_impossible_travel:
                    fraud_score += 30
                if is_amount_anomaly:
                    fraud_score += 25
                if prev_ip_changes >= 5:
                    fraud_score += 15
                if trans_last_hour >= 10:
                    fraud_score += 10
                fraud_score = min(fraud_score, 100.0)
                
                # Fraud prediction
                is_fraud_pred = 1 if fraud_score >= 50 else 0
                
                # Append result
                results.append({
                    'transaction_id': row['transaction_id'],
                    'user_id': user_id,
                    'timestamp': current_time,
                    'amount': current_amount,
                    'merchant_id': row['merchant_id'],
                    'ip_address': current_ip,
                    'latitude': current_lat,
                    'longitude': current_lon,
                    'user_transaction_count': prev_count,
                    'transactions_last_hour': trans_last_hour,
                    'transactions_last_10min': trans_last_10min,
                    'ip_changed': ip_changed,
                    'ip_change_count_total': prev_ip_changes,
                    'distance_from_last_km': distance_km,
                    'velocity_kmh': velocity_kmh,
                    'amount_vs_user_avg_ratio': amount_vs_avg_ratio,
                    'amount_vs_user_max_ratio': amount_vs_max_ratio,
                    'amount_zscore': amount_zscore,
                    'seconds_since_last_transaction': time_diff,
                    'is_rapid_transaction': is_rapid,
                    'is_impossible_travel': is_impossible_travel,
                    'is_amount_anomaly': is_amount_anomaly,
                    'fraud_score': fraud_score,
                    'is_fraud_prediction': is_fraud_pred
                })
                
                # Update state for next transaction
                prev_last_time = current_time
                prev_ip = current_ip
                prev_lat = current_lat
                prev_lon = current_lon
            
            # Update SINGLE consolidated state object (atomic update)
            self.user_state.update((
                prev_count,           # transaction_count
                prev_last_time,       # last_timestamp
                prev_ip,              # last_ip_address
                prev_lat,             # last_latitude
                prev_lon,             # last_longitude
                prev_ip_changes,      # ip_change_count
                prev_total_amount,    # total_amount
                prev_avg_amount,      # avg_amount
                prev_max_amount,      # max_amount
                prev_times,           # recent_timestamps
                prev_amounts          # recent_amounts
            ))
            
            # Yield results
            if results:
                yield pd.DataFrame(results)
    
    def handleExpiredTimer(self, key, timer_values, expired_timer_info):
        """
        Handle expired timers (not used in this implementation).
        
        Args:
            key: user_id
            timer_values: Timer values
            expired_timer_info: Information about expired timer
        
        Yields:
            pd.DataFrame: Empty DataFrame
        """
        import pandas as pd
        # No timer logic in this basic implementation
        yield pd.DataFrame()
    
    def close(self) -> None:
        """
        Perform cleanup operations (none needed).
        """
        pass


if __name__ == "__main__":
    main()