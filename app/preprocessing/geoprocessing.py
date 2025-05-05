import logging
import geopandas as gpd

logger = logging.getLogger(__name__)

def process_geometries(gdf):
    logger.info("Processing geometries. Checking geometry types...")
    geom_types = gdf.geometry.geom_type.value_counts()
    logger.debug("Geometry types found: %s", geom_types.to_dict())

    if gdf.geometry.geom_type.isin(['Polygon', 'MultiPolygon']).any():
        logger.info("Some geometries are polygons/multipolygons. Calculating centroids...")
        if gdf.crs is None:
            logger.warning("No CRS set. Setting to WGS84 (EPSG:4326).")
            gdf.set_crs(epsg=4326, inplace=True)

        # Convert to projected coordinate system
        gdf = gdf.to_crs(epsg=3857)
        gdf['geometry'] = gdf.centroid
        # Convert back to WGS84
        gdf = gdf.to_crs(epsg=4326)
        logger.info("Centroids calculated and CRS reprojected back to EPSG:4326.")
    else:
        logger.info("No polygon geometries found; skipping centroid calculation.")

    return gdf
