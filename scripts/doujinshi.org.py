# This script processes doujinshi.org raw datasets
# and converts them into csv files for easier analysis,
# then inserts them into a PostgreSQL database.

import polars as pl
import glob
import orjson
from tqdm import tqdm
import concurrent.futures


def read_json(file_path):
    """Read a JSON file and return a DataFrame."""
    with open(file_path, "rb") as f:
        data = orjson.loads(f.read())
    df = pl.json_normalize(data, strict=False)
    return df


def process_circles(position=0):
    """Process all JSON files in a directory."""
    dfs = []
    for file_path in tqdm(
        glob.glob("data/doujinshi.org/Circle/*.json"),
        desc="Processing Circles",
        position=position,
    ):
        df = read_json(file_path)
        df = df.select(
            [pl.col("@ID"), pl.col("NAME_JP"), pl.col("NAME_EN"), pl.col("NAME_R")]
        )
        dfs.append(df)

    # Concatenate all DataFrames into one
    df = pl.concat(dfs, how="diagonal_relaxed")
    df = df.rename(
        {"@ID": "id", "NAME_JP": "name", "NAME_EN": "name_en", "NAME_R": "name_romaji"}
    )
    df = df.with_columns(pl.col("id").str.replace("C", ""))

    df.write_csv("data/doujinshi.org/circles.csv")


def process_authors(position=0):
    """Process all JSON files in a directory."""
    dfs = []
    for file_path in tqdm(
        glob.glob("data/doujinshi.org/Author/*.json"),
        desc="Processing Authors",
        position=position,
    ):
        df = read_json(file_path)
        df = df.select(
            [pl.col("@ID"), pl.col("NAME_JP"), pl.col("NAME_EN"), pl.col("NAME_R")]
        )
        dfs.append(df)

    # Concatenate all DataFrames into one
    df = pl.concat(dfs, how="diagonal_relaxed")
    df = df.rename(
        {"@ID": "id", "NAME_JP": "name", "NAME_EN": "name_en", "NAME_R": "name_romaji"}
    )
    df = df.with_columns(pl.col("id").str.replace("A", ""))

    df.write_csv("data/doujinshi.org/authors.csv")


if __name__ == "__main__":
    with concurrent.futures.ProcessPoolExecutor() as executor:
        concurrent.futures.wait(
            [
                executor.submit(process_authors, 0),
                executor.submit(process_circles, 1),
            ]
        )
