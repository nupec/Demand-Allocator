import pandas as pd

from app.methods.real_distance_pandana_method import real_distance_pandana_method
from app.methods.geodesic_method import geodesic_method



def allocate_demands_knn(
    demands_gdf, opportunities_gdf, col_demand_id, col_name, col_city, k=1, method="geodesic", city_name=None, num_threads=1
):
    """
    Allocate demands using KNN. Supports geodesic and real distance methods.
    """
    if method == "geodesic":
        return geodesic_method(demands_gdf, opportunities_gdf, col_demand_id, col_name, col_city)
    elif method == "pandana_real_distance":
        return real_distance_pandana_method(
            demands_gdf, opportunities_gdf, col_demand_id, col_name, col_city, city_name=city_name, num_threads=num_threads
        )
    else:
        raise ValueError("Invalid method. Use 'geodesic' or 'pandana_real_distance'.")
