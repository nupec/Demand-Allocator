# Demand-Allocator

Welcome to the **Demand-Allocator** repository! This project provides a FastAPI-based application designed to allocate demands to different establishments based on geographic data using geodesic distance calculations.

## Features

- **Geodesic Distance Calculation**: Efficiently calculates the geodesic distance between demand points and establishments.
- **Centroid Calculation**: Automatically calculates centroids for polygon geometries, ensuring accurate distance measurements when dealing with polygons or multipolygons.
- **K-Nearest Neighbors (KNN) Allocation**: Supports demand allocation using KNN, with configurable `k` for either demands or establishments.
- **Flexible Configuration**: Easily configure which columns to use for demands and establishments through the `config.py` file.
- **Modular Structure**: Organized into modules for easy maintenance, extensibility, and scalability.

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
- libpysal
- python-multipart


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
│   │   ├── allocation.py        # Logic for demand allocation using geodesic distance and KNN
│   │   ├── common.py            # Shared logic for preparing the data for allocation
│   │   └── __init__.py
│   ├── config.py                # Application configuration and settings
│   ├── geoprocessing/
│   │   ├── geoprocessing.py     # Handles centroid and geometry processing
│   │   └── __init__.py
│   ├── main.py                  # The entry point where FastAPI is initialized
│   ├── routes/
│   │   ├── allocation_route.py  # API route for demand allocation using geodesic distance
│   │   ├── knn_route.py         # API route for demand allocation using KNN
│   │   └── __init__.py
│   └── utils/
│       ├── utils.py             # Utility functions for geodesic calculations and column inference
│       ├── __init__.py
├── LICENSE
├── README.md                    # Documentation for setting up and running the project
├── requirements.txt             # Dependencies required for the project
└── run.py                       # Script to run the application
```
- app/main.py: The entry point of the application where the FastAPI server is initialized.

- app/config.py: Contains the application settings, including possible columns for demands and establishments.

- app/allocation/allocation.py: Contains the logic for allocating demands to the nearest establishment using both geodesic distance and KNN.

- app/allocation/common.py: Prepares the data for allocation, handling centroids and filtering data based on state and city.

- app/geoprocessing/geoprocessing.py: Handles geometric data processing, including the calculation of centroids for polygon geometries.

- app/routes/allocation_route.py: API endpoint for allocating demands to establishments based on geodesic distance.

- app/routes/knn_route.py: API endpoint for allocating demands to establishments using the KNN algorithm.

- app/utils/utils.py: Utility functions, such as column name inference and geodesic distance calculation.
