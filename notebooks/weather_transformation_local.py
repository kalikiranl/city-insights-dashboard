from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, 
    avg, max, min, count, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, TimestampType
)
import os

# Enhanced schema for weather data
weather_schema = StructType([
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("weather_description", StringType(), True),
    StructField("weather_main", StringType(), True),
    # New fields
    StructField("uv_index", DoubleType(), True),
    StructField("precipitation_prob", DoubleType(), True),
    StructField("air_quality_index", DoubleType(), True)
])

def create_spark_session():
    """Create a Spark session with Delta Lake support."""
    return (SparkSession.builder
            .appName("WeatherDataTransformation")
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", "spark-warehouse")
            .master("local[*]")
            .getOrCreate())

def read_bronze_data(spark, input_path):
    """Read raw weather data from local bronze layer."""
    return (spark.readStream
           .format("json")
           .schema(weather_schema)
           .load(input_path))

def calculate_feels_like(temperature, humidity, wind_speed):
    """
    Calculate feels-like temperature using a simplified version of the heat index
    and wind chill formulas.
    """
    return f"""
        CASE
            WHEN temperature > 20 THEN  /* Hot weather - use heat index */
                temperature + (0.348 * humidity) - (0.7 * wind_speed)
            WHEN temperature < 10 THEN  /* Cold weather - use wind chill */
                13.12 + 0.6215 * temperature - 11.37 * POWER(wind_speed, 0.16) + 0.3965 * temperature * POWER(wind_speed, 0.16)
            ELSE temperature  /* Moderate weather - use actual temperature */
        END
    """

def transform_weather_data(df):
    """Apply enhanced transformations to weather data."""
    return (df
            # Convert timestamp string to timestamp type
            .withColumn("timestamp", to_timestamp("timestamp"))
            
            # Convert temperature to Fahrenheit
            .withColumn("temperature_f", expr("temperature * 9/5 + 32"))
            
            # Add feels-like temperature
            .withColumn("feels_like", expr(calculate_feels_like("temperature", "humidity", "wind_speed")))
            .withColumn("feels_like_f", expr("feels_like * 9/5 + 32"))
            
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
            
            # Add UV index risk category
            .withColumn("uv_risk_category",
                       expr("""
                           CASE
                               WHEN uv_index >= 11 THEN 'Extreme'
                               WHEN uv_index >= 8 THEN 'Very High'
                               WHEN uv_index >= 6 THEN 'High'
                               WHEN uv_index >= 3 THEN 'Moderate'
                               ELSE 'Low'
                           END
                       """))
            
            # Add precipitation probability category
            .withColumn("precipitation_category",
                       expr("""
                           CASE
                               WHEN precipitation_prob >= 0.7 THEN 'High Chance'
                               WHEN precipitation_prob >= 0.3 THEN 'Medium Chance'
                               ELSE 'Low Chance'
                           END
                       """))
            
            # Add air quality interpretation
            .withColumn("air_quality_category",
                       expr("""
                           CASE
                               WHEN air_quality_index > 300 THEN 'Hazardous'
                               WHEN air_quality_index > 200 THEN 'Very Unhealthy'
                               WHEN air_quality_index > 150 THEN 'Unhealthy'
                               WHEN air_quality_index > 100 THEN 'Unhealthy for Sensitive Groups'
                               WHEN air_quality_index > 50 THEN 'Moderate'
                               ELSE 'Good'
                           END
                       """))
    )

def calculate_aggregations(df):
    """Calculate enhanced hourly aggregations by city."""
    return (df
            .withWatermark("timestamp", "1 hour")
            .groupBy(
                window("timestamp", "1 hour"),
                "city",
                "country"
            )
            .agg(
                # Temperature metrics
                avg("temperature").alias("avg_temperature"),
                max("temperature").alias("max_temperature"),
                min("temperature").alias("min_temperature"),
                avg("feels_like").alias("avg_feels_like"),
                
                # Other weather metrics
                avg("humidity").alias("avg_humidity"),
                avg("pressure").alias("avg_pressure"),
                avg("wind_speed").alias("avg_wind_speed"),
                avg("uv_index").alias("avg_uv_index"),
                avg("precipitation_prob").alias("avg_precipitation_prob"),
                avg("air_quality_index").alias("avg_air_quality_index"),
                
                # Count of readings
                count("*").alias("number_of_readings")
            ))

def write_to_silver_layer(df, output_path):
    """Write transformed data to local silver layer using Delta format."""
    checkpoint_path = os.path.join(output_path, "../checkpoints/silver/weather")
    
    return (df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .start(output_path))

def write_aggregations_to_gold(df, output_path):
    """Write aggregated data to local gold layer using Delta format."""
    checkpoint_path = os.path.join(output_path, "../checkpoints/gold/weather")
    
    return (df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .start(output_path))

def main():
    """Main function to run the transformation pipeline."""
    # Create Spark session
    spark = create_spark_session()
    
    # Configuration - local paths
    input_path = "data/bronze/weather"  # Directory containing JSON files
    silver_output_path = "data/silver/weather"
    gold_output_path = "data/gold/weather_aggregations"
    
    # Create output directories if they don't exist
    os.makedirs(input_path, exist_ok=True)
    os.makedirs(silver_output_path, exist_ok=True)
    os.makedirs(gold_output_path, exist_ok=True)
    
    try:
        # Read bronze data
        bronze_df = read_bronze_data(spark, input_path)
        
        # Transform data
        silver_df = transform_weather_data(bronze_df)
        
        # Calculate aggregations
        gold_df = calculate_aggregations(silver_df)
        
        # Write to silver layer
        silver_query = write_to_silver_layer(silver_df, silver_output_path)
        
        # Write aggregations to gold layer
        gold_query = write_aggregations_to_gold(gold_df, gold_output_path)
        
        # Await termination
        spark.streams.awaitAnyTermination()
        
    except Exception as e:
        print(f"Error in transformation pipeline: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 