import geopandas as gpd

def process_geometries(gdf):
    # Print the geometry type for debugging
    print("Geometries in the DataFrame: ", gdf.geometry.geom_type.value_counts())
    
    # Check if there are Polygons or MultiPolygons
    if gdf.geometry.geom_type.isin(['Polygon', 'MultiPolygon']).any():
        print("Calculating centroids for geometries...")

        # Check if CRS is defined, if not, set it to WGS84
        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)
            print("CRS was not set, setting it to WGS84 (EPSG:4326)")

        # Convert to a projected coordinate system (3857) to correctly calculate centroids
        gdf = gdf.to_crs(epsg=3857)
        gdf['geometry'] = gdf.centroid
        # Convert back to WGS84 (EPSG:4326)
        gdf = gdf.to_crs(epsg=4326)

        print("Centroids calculated and CRS converted back to WGS84.")
    else:
        print("No centroid calculation needed.")
    
    return gdf
