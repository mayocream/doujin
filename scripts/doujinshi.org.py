# This script processes doujinshi.org raw datasets
# and converts them into csv files for easier analysis,
# then inserts them into a PostgreSQL database.

import polars as pl
import glob
import os
import orjson
from tqdm import tqdm


def read_json(file_path):
    """Read a JSON file and return a DataFrame."""
    with open(file_path, 'rb') as f:
        data = orjson.loads(f.read())
    df = pl.json_normalize(data, max_level=0, strict=False)
    df = df.drop("url")
    if "NAME_ALT" in df.columns:
        df = df.drop("NAME_ALT")

    if "LINKS" in df.columns:
        df = df.drop("LINKS")

    return df

def process_directory(directory_path, output_path):
    """Process all JSON files in a directory."""
    dfs = []
    for file_path in tqdm(glob.glob(os.path.join(directory_path, "*.json"))):
        df = read_json(file_path)

        dfs.append(df)

    # Concatenate all DataFrames into one
    df = pl.concat(dfs, how="diagonal_relaxed")
    df.write_csv(output_path)

if __name__ == "__main__":
    process_directory("data/doujinshi.org/Author", "data/doujinshi.org/Author.csv")
