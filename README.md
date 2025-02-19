# Demand-Allocator

Welcome to the **Demand-Allocator** repository! This project provides a FastAPI-based application designed to allocate demands to different establishments based on geographic data using geodesic distance calculations and network analysis.

## Features

- **Geodesic Distance Calculation**: Efficiently calculates the geodesic distance between demand points and establishments.
- **Centroid Calculation**: Automatically calculates centroids for polygon geometries, ensuring accurate distance measurements when dealing with polygons or multipolygons.
- **K-Nearest Neighbors (KNN) Allocation**: Supports demand allocation using KNN, with configurable k for either demands or establishments.
- **Network Distance Calculation**: Utilizes road networks to compute a more realistic distance matrix between demands and establishments.
- **Flexible Configuration**: Easily configure which columns to use for demands and establishments through the `config.py` file.
- **Modular Structure**: Organized into modules for easy maintenance, extensibility, and scalability.

## Installation

### Prerequisites

- Python 3.8+
- Conda (environment manager)
- Git (to clone the repository)

### Clone the Repository

```bash
git clone https://github.com/nupec/Demand-Allocator.git
cd Demand-Allocator
```

## Create a Conda Virtual Environment

### It's recommended to use a conda environment to manage dependencies:

```bash
conda env create -f environment.yml
```
## Activate the Conda Environment:
## On Linux/macOS and Windows:

```bash
conda activate demand-allocator
```

## Environment Setup
### The **environment.yml** file includes 
name: demand-allocator

channels:
  - conda-forge

dependencies:
  - geopandas
  - geopy
  - pandas
  - fastapi
  - unidecode
  - uvicorn
  - libpysal
  - python-multipart
  - osmnx
  - pandana
  - numpy
  - matplotlib


## Run the Application

### Start the FastAPI server:
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```
The application will be available at http://0.0.0.0:8000/docs.

### Deactivate the Conda Environment

When you're done working, you can deactivate the environment:
## On Linux/macOS and Windows:
```bash
conda deactivate
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
│   ├── network_analysis/
│   │   ├── __init__.py          # Imports compute_distance_matrix from network.py
│   │   └── network.py            # Functions for computing distance matrix and network analysis
│   ├── main.py                  # The entry point where FastAPI is initialized
│   ├── routes/
│   │   ├── allocation_route.py  # API route for demand allocation using geodesic distance
│   │   ├── distance_matrix_route.py # API route for computing distance matrices
│   │   ├── knn_route.py         # API route for demand allocation using KNN
│   │   └── __init__.py
│   └── utils/
│       ├── __init__.py
│       └── utils.py             # Utility functions for geodesic calculations and column inference
├── environment.yml              # Conda environment configuration
├── LICENSE
├── README.md                    # Documentation for setting up and running the project
└── run.py                       # Script to run the application
```
- app/main.py: The entry point of the application where the FastAPI server is initialized.

- app/config.py: Contains the application settings, including possible columns for demands and establishments.

- app/allocation/allocation.py: Contains the logic for allocating demands to the nearest establishment using both geodesic distance and KNN.

- app/allocation/common.py: Prepares the data for allocation, handling centroids and filtering data based on state and city.

- app/geoprocessing/geoprocessing.py: Handles geometric data processing, including the calculation of centroids for polygon geometries.

- app/network_analysis/network.py: Contains logic for calculating a distance matrix using road network data, with Pandana and OSMnx.

- app/routes/distance_matrix_route.py: API endpoint for calculating a distance matrix between demands and establishments using network analysis.

- app/routes/allocation_route.py: API endpoint for allocating demands to establishments based on geodesic distance.

- app/routes/knn_route.py: API endpoint for allocating demands to establishments using the KNN algorithm.

- app/utils/utils.py: Utility functions, such as column name inference and geodesic distance calculation.


## Test Data

To facilitate testing and validation of the Demand-Allocator system, we provide sample GeoJSON files in a structured format. These files represent synthetic demand points and establishment locations in different configurations.
Sample Files Structure


```bash
test_data/
├── 10x10_RL_demands.geojson         # 10x10 grid of demand points (Rio Largo example)
├── 10x10_RL_establishments.geojson  # Corresponding establishments for Rio Largo
├── 5x5_AM_demands.geojson           # 5x5 grid of demand points (Amazonas example)
└── 5x5_AM_establishments.geojson    # Corresponding establishments for Amazonas
