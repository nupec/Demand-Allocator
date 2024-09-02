import geopandas as gpd

def process_geometries(gdf):
    if gdf.geometry.geom_type.isin(['Polygon', 'MultiPolygon']).any():
        print("Calculating centroids for geometries...")
        gdf = gdf.to_crs(epsg=3857)  
        gdf['geometry'] = gdf.centroid
        gdf = gdf.to_crs(epsg=4326) 
        print("Centroids calculated and CRS converted back to WGS84.")
    else:
        print("No centroid calculation needed.")
    return gdf
