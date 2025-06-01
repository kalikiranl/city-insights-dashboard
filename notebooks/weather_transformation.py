from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, 
    avg, max, min, count, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, TimestampType
)

# Define schema for weather data
weather_schema = StructType([
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("weather_description", StringType(), True),
    StructField("weather_main", StringType(), True)
])

def create_spark_session():
    """Create a Spark session with Delta Lake support."""
    return (SparkSession.builder
            .appName("WeatherDataTransformation")
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())

def read_bronze_data(spark, storage_account, container):
    """Read raw weather data from bronze layer (Data Lake)."""
    bronze_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/raw-weather-data"
    
    return (spark.readStream
           .format("json")
           .schema(weather_schema)
           .load(bronze_path))

def transform_weather_data(df):
    """Apply transformations to weather data."""
    return (df
            # Convert timestamp string to timestamp type
            .withColumn("timestamp", to_timestamp("timestamp"))
            
            # Convert temperature to Fahrenheit
            .withColumn("temperature_f", expr("temperature * 9/5 + 32"))
            
            # Add weather severity category
            .withColumn("weather_severity", 
                       expr("""
                           CASE 
                               WHEN weather_main IN ('Thunderstorm', 'Tornado', 'Hurricane') THEN 'Severe'
                               WHEN weather_main IN ('Rain', 'Snow', 'Sleet') THEN 'Moderate'
                               ELSE 'Mild'
                           END
                       """))
            
            # Calculate wind severity
            .withColumn("wind_severity",
                       expr("""
                           CASE
                               WHEN wind_speed > 50 THEN 'Dangerous'
                               WHEN wind_speed > 30 THEN 'High'
                               WHEN wind_speed > 15 THEN 'Moderate'
                               ELSE 'Low'
                           END
                       """))
    )

def calculate_aggregations(df):
    """Calculate hourly aggregations by city."""
    return (df
            .withWatermark("timestamp", "1 hour")
            .groupBy(
                window("timestamp", "1 hour"),
                "city",
                "country"
            )
            .agg(
                avg("temperature").alias("avg_temperature"),
                max("temperature").alias("max_temperature"),
                min("temperature").alias("min_temperature"),
                avg("humidity").alias("avg_humidity"),
                avg("pressure").alias("avg_pressure"),
                avg("wind_speed").alias("avg_wind_speed"),
                count("*").alias("number_of_readings")
            ))

def write_to_silver_layer(df, storage_account, container):
    """Write transformed data to silver layer using Delta format."""
    silver_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/silver/weather"
    
    checkpoint_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/checkpoints/silver/weather"
    
    return (df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .start(silver_path))

def write_aggregations_to_gold(df, storage_account, container):
    """Write aggregated data to gold layer using Delta format."""
    gold_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/gold/weather_aggregations"
    
    checkpoint_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/checkpoints/gold/weather"
    
    return (df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .start(gold_path))

def main():
    """Main function to run the transformation pipeline."""
    # Create Spark session
    spark = create_spark_session()
    
    # Configuration
    storage_account = "your_storage_account_name"
    container = "your_container_name"
    
    try:
        # Read bronze data
        bronze_df = read_bronze_data(spark, storage_account, container)
        
        # Transform data
        silver_df = transform_weather_data(bronze_df)
        
        # Calculate aggregations
        gold_df = calculate_aggregations(silver_df)
        
        # Write to silver layer
        silver_query = write_to_silver_layer(silver_df, storage_account, container)
        
        # Write aggregations to gold layer
        gold_query = write_aggregations_to_gold(gold_df, storage_account, container)
        
        # Await termination
        spark.streams.awaitAnyTermination()
        
    except Exception as e:
        print(f"Error in transformation pipeline: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 