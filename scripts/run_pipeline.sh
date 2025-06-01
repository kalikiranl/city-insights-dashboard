#!/bin/bash

# Create necessary directories
mkdir -p data/bronze/weather
mkdir -p data/silver/weather
mkdir -p data/gold/weather_aggregations
mkdir -p spark-warehouse

# Run the pipeline
python3 notebooks/weather_transformation_local.py 