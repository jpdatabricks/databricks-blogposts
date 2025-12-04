class Config:
    """
    Configuration class for application settings.
    """

    def __init__(self):
        """
        Initializes the configuration with default values.
        """

        # Kafka configuration for streaming data ingestion
        self.kafka_config = {
            "kafka_topic": "transactions_source_stream",
            "kafka_credentials_secrets": {
                "scope": "jaypalaniappan",
                "key": "KAFKA_KEY",
                "server": "KAFKA_SERVER",
                "secret": "KAFKA_SECRET",
            }
        }
        
        # Data generation configuration for synthetic data
        self.data_gen_config = {
            "num_users": 10000,
            "num_merchants": 1000,
            "rows_per_second": 1000
        }
        
        # Lakebase configuration for data storage
        self.lakebase_config = {
            "instance_name": "rtm-lakebase-demo",
            "database": "databricks_postgres"
        }

# Example usage
if __name__ == "__main__":
    print("Testing config reader...")
    config = Config()
    print(config.kafka_config)
    print(config.data_gen_config)
    print(config.lakebase_config)
    print("\nTest complete!")