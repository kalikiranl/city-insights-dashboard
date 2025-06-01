import os
import json
import requests
from datetime import datetime
from azure.storage.blob import BlobServiceClient

class WeatherDataIngestion:
    def __init__(self):
        """Initialize the weather data ingestion service."""
        self.api_key = os.environ.get('OPENWEATHER_API_KEY')
        self.connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        
        # List of cities to monitor
        self.cities = [
            {"name": "London", "country": "UK"},
            {"name": "New York", "country": "US"},
            {"name": "Tokyo", "country": "JP"},
            {"name": "Singapore", "country": "SG"}
        ]

    def fetch_weather_data(self, city):
        """Fetch weather data from OpenWeather API."""
        url = "http://api.openweathermap.org/data/2.5/weather"
        params = {
            'q': f"{city['name']},{city['country']}",
            'appid': self.api_key,
            'units': 'metric'
        }
        
        response = requests.get(url, params=params)
        return response.json()

    def process_weather_data(self, raw_data):
        """Process raw weather data into structured format."""
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
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        container_client = blob_service_client.get_container_client("weather-data")
        
        # Create blob name with city and timestamp
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        blob_name = f"{city['name'].lower()}/{timestamp}.json"
        
        # Upload data as JSON
        container_client.upload_blob(
            name=blob_name,
            data=json.dumps(data),
            overwrite=True
        )

    def run_ingestion(self):
        """Run the complete ingestion process for all cities."""
        for city in self.cities:
            try:
                # Fetch and process data
                raw_data = self.fetch_weather_data(city)
                processed_data = self.process_weather_data(raw_data)
                
                # Upload to blob storage
                self.upload_to_blob(processed_data, city)
                
            except Exception as e:
                print(f"Error processing {city['name']}: {str(e)}")

if __name__ == "__main__":
    ingestion = WeatherDataIngestion()
    ingestion.run_ingestion() 