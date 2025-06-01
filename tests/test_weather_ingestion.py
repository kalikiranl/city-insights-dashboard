import unittest
from unittest.mock import patch, MagicMock
import json
from datetime import datetime
import os
import sys

# Add the scripts directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from weather_ingestion import WeatherDataIngestion

class TestWeatherDataIngestion(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        # Mock environment variables with properly formatted connection string
        self.mock_connection_string = 'DefaultEndpointsProtocol=https;AccountName=fakestorage;AccountKey=fake+key+with+base64==;EndpointSuffix=core.windows.net'
        self.env_patcher = patch.dict('os.environ', {
            'OPENWEATHER_API_KEY': 'fake_api_key',
            'AZURE_STORAGE_CONNECTION_STRING': self.mock_connection_string
        })
        self.env_patcher.start()
        
        self.ingestion = WeatherDataIngestion()
        
        # Sample weather data response
        self.sample_weather_data = {
            "name": "London",
            "sys": {"country": "UK"},
            "main": {
                "temp": 15.5,
                "humidity": 75,
                "pressure": 1012
            },
            "wind": {"speed": 5.5},
            "weather": [{
                "description": "scattered clouds",
                "main": "Clouds"
            }]
        }

    def tearDown(self):
        """Clean up test fixtures."""
        self.env_patcher.stop()

    def test_init(self):
        """Test initialization of WeatherDataIngestion class."""
        self.assertEqual(self.ingestion.api_key, 'fake_api_key')
        self.assertEqual(self.ingestion.connection_string, self.mock_connection_string)
        self.assertEqual(len(self.ingestion.cities), 4)  # Check if all cities are included

    @patch('requests.get')
    def test_fetch_weather_data(self, mock_get):
        """Test weather data fetching."""
        # Configure the mock
        mock_response = MagicMock()
        mock_response.json.return_value = self.sample_weather_data
        mock_get.return_value = mock_response

        # Test successful API call
        result = self.ingestion.fetch_weather_data({"name": "London", "country": "UK"})
        self.assertEqual(result, self.sample_weather_data)

        # Verify the API was called with correct parameters
        mock_get.assert_called_with(
            "http://api.openweathermap.org/data/2.5/weather",
            params={
                'q': 'London,UK',
                'appid': 'fake_api_key',
                'units': 'metric'
            }
        )

    def test_process_weather_data(self):
        """Test weather data processing."""
        processed_data = self.ingestion.process_weather_data(self.sample_weather_data)
        
        self.assertEqual(processed_data['city'], 'London')
        self.assertEqual(processed_data['country'], 'UK')
        self.assertEqual(processed_data['temperature'], 15.5)
        self.assertEqual(processed_data['humidity'], 75)
        self.assertEqual(processed_data['pressure'], 1012)
        self.assertEqual(processed_data['wind_speed'], 5.5)
        self.assertEqual(processed_data['weather_description'], 'scattered clouds')
        self.assertEqual(processed_data['weather_main'], 'Clouds')

    @patch('azure.storage.blob.BlobClient.upload_blob')
    @patch('azure.storage.blob.BlobServiceClient.from_connection_string')
    def test_upload_to_blob(self, mock_from_connection_string, mock_upload_blob):
        """Test blob storage upload."""
        # Configure the mock container client
        mock_container_client = MagicMock()
        mock_blob_client = MagicMock()
        
        # Set up the chain of mocks
        mock_blob_service_instance = MagicMock()
        mock_from_connection_string.return_value = mock_blob_service_instance
        mock_blob_service_instance.get_container_client.return_value = mock_container_client
        mock_container_client.get_blob_client.return_value = mock_blob_client
        mock_upload_blob.return_value = None  # Mock successful upload
        
        # Test data
        test_data = {
            'city': 'London',
            'temperature': 20.5,
            'timestamp': '2024-03-21T12:00:00'
        }
        
        # Test upload
        self.ingestion.upload_to_blob(test_data, {"name": "London", "country": "UK"})
        
        # Verify the blob service client was created with correct connection string
        mock_from_connection_string.assert_called_once_with(self.mock_connection_string)
        
        # Verify container client was created with correct name
        mock_blob_service_instance.get_container_client.assert_called_once_with("weather-data")
        
        # Verify blob client was created with correct name pattern
        mock_container_client.get_blob_client.assert_called_once()
        blob_name_arg = mock_container_client.get_blob_client.call_args[0][0]
        self.assertRegex(blob_name_arg, r'london/\d{8}_\d{6}\.json')
        
        # Verify upload_blob was called with correct data
        mock_blob_client.upload_blob.assert_called_once()
        call_args = mock_blob_client.upload_blob.call_args
        self.assertEqual(json.loads(call_args[0][0]), test_data)  # First arg is data
        self.assertEqual(call_args[1], {'overwrite': True})  # Second arg is kwargs

    @patch.object(WeatherDataIngestion, 'fetch_weather_data')
    @patch.object(WeatherDataIngestion, 'upload_to_blob')
    def test_run_ingestion(self, mock_upload, mock_fetch):
        """Test the complete ingestion process."""
        # Configure mocks
        mock_fetch.return_value = self.sample_weather_data
        
        # Run ingestion
        self.ingestion.run_ingestion()
        
        # Verify all cities were processed
        self.assertEqual(mock_fetch.call_count, 4)  # Should be called for each city
        self.assertEqual(mock_upload.call_count, 4)  # Should upload data for each city

if __name__ == '__main__':
    unittest.main() 