import logging
from unidecode import unidecode

logger = logging.getLogger(__name__)

def infer_column(gdf, possible_names):
    logger.debug("Inferring column from possible names: %s", possible_names)
    for name in possible_names:
        columns = [col for col in gdf.columns if unidecode(col).lower() == unidecode(name).lower()]
        if columns:
            logger.info("Inferred column '%s' from the provided list: %s", columns[0], possible_names)
            return columns[0]
    logger.warning("No column found for the possible names: %s", possible_names)
    return None
