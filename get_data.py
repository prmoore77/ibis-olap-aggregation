from config import logger
import os
import wget
import tempfile
import zipfile


def get_data():
    with tempfile.TemporaryDirectory() as temp_dir:
        logger.info(msg=f"Downloading to directory: {temp_dir}")
        wget.download(url="https://drive.google.com/uc?export=download&id=1-1FautXBDrF4qbIvpcyGBI6WQ6Fgr8D2",
                      out=temp_dir
                      )
        with zipfile.ZipFile(os.path.join(temp_dir, "grocery_store.duckdb.zip"), "r") as zip_file:
            zip_file.extractall("./data")


if __name__ == '__main__':
    get_data()
