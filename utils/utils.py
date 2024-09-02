from unidecode import unidecode
from geopy.distance import geodesic

def infer_column(gdf, possible_names):
    for name in possible_names:
        columns = [col for col in gdf.columns if unidecode(col).lower() == unidecode(name).lower()]
        if columns:
            print(f"Inferred column: {columns[0]} for name: {name}")
            return columns[0]
    print(f"No corresponding column found for the possible names: {possible_names}")
    return None

def calculate_geodesic_distance(point1, point2):
    return geodesic(point1, point2).kilometers
