# This script processes doujinshi.org raw datasets
# and converts them into csv files for easier analysis,
# then inserts them into a PostgreSQL database.

import polars as pl
import glob
import orjson
from tqdm import tqdm
import concurrent.futures
from pprint import pprint


def read_json(file_path):
    """Read a JSON file and return a DataFrame."""
    with open(file_path, "rb") as f:
        data = orjson.loads(f.read())
    df = pl.json_normalize(data, strict=False)
    return df


def process_books(position=0):
    # TODO
    return None


def process_contents(position=0):
    """Process all JSON files in a directory."""
    dfs = []
    for file_path in tqdm(
        glob.glob("data/doujinshi.org/Content/*.json"),
        desc="Processing Contents",
        position=position,
    ):
        df = read_json(file_path)
        df = df.select(
            [
                pl.col("@ID"),
                pl.col("NAME_JP"),
                pl.col("NAME_EN"),
                pl.col("NAME_R"),
            ]
        )
        dfs.append(df)

    # Concatenate all DataFrames into one
    df = pl.concat(dfs, how="diagonal_relaxed")
    df = df.rename(
        {
            "@ID": "id",
            "NAME_JP": "name",
            "NAME_EN": "name_en",
            "NAME_R": "name_romaji",
        }
    )
    df = df.with_columns(pl.col("id").str.replace("K", ""))

    df.write_csv("data/doujinshi.org/contents.csv")


def process_characters(position=0):
    """Process all JSON files in a directory."""
    dfs = []
    for file_path in tqdm(
        glob.glob("data/doujinshi.org/Character/*.json"),
        desc="Processing Characters",
        position=position,
    ):
        df = read_json(file_path)
        tags = []
        if "LINKS.ITEM" in df.columns:
            tags = [x.get("@ID") for sublist in df["LINKS.ITEM"] for x in sublist]

        df = df.with_columns(pl.lit(tags).alias("tags"))
        df = df.select(
            [
                pl.col("@ID"),
                pl.col("NAME_JP"),
                pl.col("NAME_EN"),
                pl.col("NAME_R"),
                pl.col("DATA_SEX"),
                pl.col("DATA_AGE"),
                pl.col("tags"),
            ]
        )
        dfs.append(df)

    # Concatenate all DataFrames into one
    df = pl.concat(dfs)
    df = df.rename(
        {
            "@ID": "id",
            "NAME_JP": "name",
            "NAME_EN": "name_en",
            "NAME_R": "name_romaji",
            "DATA_SEX": "sex",
            "DATA_AGE": "age",
        }
    )
    df = df.with_columns(pl.col("id").str.replace("H", ""))

    df.write_csv("data/doujinshi.org/characters.csv")


# We ignore the relationships of circles and authors for lack of data
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
                # executor.submit(process_authors, 0),
                # executor.submit(process_circles, 1),
                executor.submit(process_characters, 2),
                # executor.submit(process_contents, 3),
            ]
        )
