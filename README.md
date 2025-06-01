# City Insights Dashboard 🌆

A real-time environmental and utility data platform that aggregates, transforms, and visualizes open data from various sources to provide insights about city metrics.

## 🎯 Project Overview

The City Insights Dashboard is a comprehensive data engineering project that processes and visualizes environmental and utility data from multiple sources. It provides real-time insights into:

- Air quality and pollution levels
- Weather conditions
- Energy consumption
- Water usage patterns
- Other environmental metrics

## 🏗️ Architecture

```
                                     City Insights Dashboard Architecture
┌──────────────┐     ┌───────────────┐     ┌─────────────┐     ┌──────────────┐     ┌───────────────┐
│  Data Sources│     │ Azure Data    │     │ Azure Data  │     │Azure Synapse │     │   Power BI    │
│  - Weather   │────▶│   Factory     │────▶│   Lake Gen2 │────▶│  Analytics   │────▶│  Dashboards   │
│  - EPA       │     │ (Ingestion)   │     │ (Storage)   │     │ (Processing) │     │               │
└──────────────┘     └───────────────┘     └─────────────┘     └──────────────┘     └───────────────┘
```

## 🛠️ Tech Stack

- **Data Ingestion**: Azure Data Factory
- **Data Storage**: Azure Data Lake Gen2, Delta Lake
- **Processing**: Azure Databricks (PySpark)
- **Analytics**: Azure Synapse Analytics
- **Visualization**: Power BI
- **CI/CD**: GitHub Actions
- **Monitoring**: Azure Monitor

## 📂 Project Structure

```
/data-insights-project
├── notebooks/              # Databricks notebooks for data processing
├── pipelines/             # Data Factory pipeline definitions
│   ├── weather/
│   ├── pollution/
│   └── utilities/
├── powerbi/               # Power BI report templates
├── scripts/               # Utility scripts and tools
├── tests/                 # Unit tests and integration tests
└── docs/                  # Project documentation
```

## 🚀 Getting Started

### Prerequisites

1. Azure Subscription
2. Python 3.8+
3. Azure CLI
4. Power BI Desktop

### Setup Instructions

1. Clone the repository:
```bash
git clone https://github.com/yourusername/city-insights-dashboard.git
cd city-insights-dashboard
```

2. Set up Python virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. Configure Azure credentials:
```bash
az login
```

## 📊 Data Sources

- OpenWeather API
- EPA AirNow API
- Local utility data (CSV/JSON formats)

## 🔄 Data Pipeline

1. **Bronze Layer (Raw)**
   - Raw data ingestion from APIs
   - Data stored in original format
   - Minimal transformations

2. **Silver Layer (Standardized)**
   - Data cleaning and normalization
   - Schema enforcement
   - Quality checks

3. **Gold Layer (Business Ready)**
   - Aggregated metrics
   - Business logic application
   - Ready for visualization

## 📈 Dashboards

- Real-time pollution monitoring
- Weather trend analysis
- Resource consumption patterns
- Regional comparisons
- Historical analysis

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📧 Contact

Your Name - your.email@example.com
Project Link: [https://github.com/yourusername/city-insights-dashboard](https://github.com/yourusername/city-insights-dashboard) 