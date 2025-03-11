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

## Test Data

To facilitate testing and validation of the Demand-Allocator system, we provide sample GeoJSON files in a structured format. These files represent synthetic demand points and establishment locations in different configurations.
Sample Files Structure


```bash
test/
    ├── 10x10_rj_demands.geojson
    ├── 10x10_rj_opportunities.geojson
    ├── 5x5_am_demands.geojson
    └── 5x5_am_opportunities.geojson

