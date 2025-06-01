import os
import json
from datetime import datetime
import requests
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

class WeatherDataIngestion:
    def __init__(self):
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        self.cities = [
            {"name": "New York", "country": "US"},
            {"name": "London", "country": "UK"},
            {"name": "Tokyo", "country": "JP"},
            {"name": "Sydney", "country": "AU"}
        ]
        
        # Azure Storage settings
        self.connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        self.container_name = 'raw-weather-data'
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)

    def fetch_weather_data(self, city):
        """Fetch weather data from OpenWeather API for a given city."""
        base_url = "http://api.openweathermap.org/data/2.5/weather"
        params = {
            'q': f"{city['name']},{city['country']}",
            'appid': self.api_key,
            'units': 'metric'
        }
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {city['name']}: {str(e)}")
            return None

    def process_weather_data(self, raw_data):
        """Transform raw weather data into structured format."""
        if not raw_data:
            return None
        
        return {
            'city': raw_data['name'],
            'country': raw_data['sys']['country'],
            'timestamp': datetime.utcnow().isoformat(),
            'temperature': raw_data['main']['temp'],
            'humidity': raw_data['main']['humidity'],
            'pressure': raw_data['main']['pressure'],
            'wind_speed': raw_data['wind']['speed'],
            'weather_description': raw_data['weather'][0]['description'],
            'weather_main': raw_data['weather'][0]['main']
        }

    def upload_to_blob(self, data, city):
        """Upload processed data to Azure Blob Storage."""
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            
            # Create blob name with timestamp
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            blob_name = f"{city['name'].lower()}/{timestamp}.json"
            
            # Convert data to JSON string
            json_data = json.dumps(data)
            
            # Upload to blob
            container_client.upload_blob(name=blob_name, data=json_data)
            print(f"Successfully uploaded data for {city['name']} to blob: {blob_name}")
            
        except Exception as e:
            print(f"Error uploading data to blob storage: {str(e)}")

    def run_ingestion(self):
        """Main method to run the ingestion process."""
        print("Starting weather data ingestion...")
        
        for city in self.cities:
            print(f"Processing {city['name']}...")
            
            # Fetch raw data
            raw_data = self.fetch_weather_data(city)
            
            if raw_data:
                # Process data
                processed_data = self.process_weather_data(raw_data)
                
                if processed_data:
                    # Upload to blob storage
                    self.upload_to_blob(processed_data, city)
            
        print("Weather data ingestion completed.")

if __name__ == "__main__":
    # Check for required environment variables
    required_env_vars = ['OPENWEATHER_API_KEY', 'AZURE_STORAGE_CONNECTION_STRING']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        exit(1)
    
    # Run ingestion
    ingestion = WeatherDataIngestion()
    ingestion.run_ingestion() 