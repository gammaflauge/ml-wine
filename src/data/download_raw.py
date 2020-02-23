import logging
from pathlib import Path
import pandas as pd

def download_raw() -> pd.DataFrame:
    """downloads the wine.data dataset from https://archive.ics.uci.edu/ml/machine-learning-databases/wine/
    
    Returns:
        pd.DataFrame -- raw wine.data with column names
    """
    logger = logging.getLogger(__name__)

    url = 'https://archive.ics.uci.edu/ml/machine-learning-databases/wine/'

    col_names = [
            "Class",
            "Alcohol",
            "Malic acid",
            "Ash",
            "Alcalinity of ash",
            "Magnesium",
            "Total phenols",
            "Flavanoids",
            "Nonflavanoid phenols",
            "Proanthocyanins",
            "Color intensity",
            "Hue",
            "OD280/OD315 of diluted wines",
            "Proline",
        ]
    logger.info(f'downloading dataset from {url}')
    return pd.read_csv(url + 'wine.data', header=None, names=col_names)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    df = download_raw()
    df.to_csv(project_dir / 'data' / 'raw' / 'wine.csv', index=False)
