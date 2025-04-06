import pandas as pd


def fix_csv():
    df = pd.read_csv(
        "data/doujinshi.org/books.csv",
        sep="\t",
        quotechar='"',
        doublequote=True,
        quoting=1,  # csv.QUOTE_ALL
        na_values=["\\N", ""],  # Better null handling
        keep_default_na=True,
    )

    # select only id and circle_id
    df = df[["id", "publisher_id"]]

    # explode circle_id
    df["publisher_id"] = df["publisher_id"].astype(str).str.split(",")
    df = df.explode("publisher_id")

    # preview
    df.to_csv(
        "data/doujinshi.org/book_publishers.csv",
        sep="\t",
        index=False,
    )


if __name__ == "__main__":
    fix_csv()
