# Demand-Allocator

Welcome to the **Demand-Allocator** repository! This project provides a FastAPI-based application designed to allocate demands to different establishments based on geographic data using geodesic distance calculations.

## Features

- **Geodesic Distance Calculation**: Efficiently calculates the geodesic distance between demand points and establishments.
- **Centroid Calculation**: Automatically calculates centroids for polygon geometries, ensuring accurate distance measurements.
- **Flexible Configuration**: Easily configure which columns to use for demands and establishments through the `config.py` file.
- **Modular Structure**: Organized into modules for easy maintenance and scalability.

## Installation

### Prerequisites

- Python 3.8+
- Pip (Python package installer)
- Git (to clone the repository)

### Clone the Repository

```bash
git clone https://github.com/nupec/Demand-Allocator.git
cd Demand-Allocator
```

## Create a Virtual Environment

### It's recommended to use a virtual environment to manage dependencies:

```bash
python3 -m venv venv
```
## Activate the Virtual Environment
### On Linux/macOS:

```bash
source venv/bin/activate
```
## On Windows:
```bash
venv\Scripts\activate
```
## Install Dependencies

### Install the required Python packages listed in requirements.txt:

```bash
pip install -r requirements.txt
```

### The requirements.txt file includes the following dependencies:

- geopandas
- geopy
- pandas
- fastapi
- unidecode
- uvicorn

## Run the Application

### Start the FastAPI server:
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```
The application will be available at http://0.0.0.0:8000.

### Deactivate the Virtual Environment

When you're done working, you can deactivate the virtual environment:
## On Linux/macOS and Windows:
```bash
deactivate
```

## Project Structure
```bash
Demand-Allocator/
├── app/
│   ├── allocation/
│   │   ├── allocation.py
│   │   └── __init__.py
│   ├── config.py
│   ├── geoprocessing/
│   │   ├── geoprocessing.py
│   │   └── __init__.py
│   ├── main.py
│   └── utils/
│       ├── utils.py
│       ├── __init__.py
│
├── run.py
├── requirements.txt
└── README.md
```
- app/main.py: The entry point of the application where the FastAPI server is initialized.

- app/config.py: Contains the application settings, including possible columns for demands and establishments.

- app/allocation/allocation.py: Contains the logic for allocating demands to the nearest establishment.

- app/geoprocessing/geoprocessing.py: Handles the processing of geometric data, including centroid calculation.

- app/utils/utils.py: Utility functions for column inference and geodesic distance calculation.
