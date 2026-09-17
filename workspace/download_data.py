import os
import urllib.request
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DataDownloader")

def download_dataset():
    """Downloads the OpenFoodFacts dataset dump locally."""
    file_path = "openfoodfacts.csv.gz"
    url = "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"

    if not os.path.exists(file_path):
        logger.info(f"Downloading dataset from {url} (This may take a few minutes)...")
        urllib.request.urlretrieve(url, file_path)
        logger.info("Dataset successfully downloaded")
    else:
        logger.info("Dataset file already exists in the workspace, skipping download")

if __name__ == "__main__":
    download_dataset()