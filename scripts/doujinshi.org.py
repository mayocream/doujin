from dotenv import load_dotenv
import os
import glob
import pandas as pd
from sqlalchemy import create_engine
import csv
from io import StringIO


load_dotenv()


# refer: https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#io-sql-method
def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join(['"{}"'.format(k) for k in keys])
        if table.schema:
            table_name = "{}.{}".format(table.schema, table.name)
        else:
            table_name = table.name

        sql = "COPY {} ({}) FROM STDIN WITH CSV".format(table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


def load_csv_to_database():
    # Create connection with SQLAlchemy
    engine = create_engine(os.getenv("DATABASE_URL"))

    # Process each CSV file
    for csv_file in glob.glob("data/doujinshi.org/*.csv"):
        table_name = os.path.basename(csv_file).replace(".csv", "")
        print(f"Processing {csv_file} to {table_name}...")

        # Read CSV with improved handling of problematic characters
        df = pd.read_csv(
            csv_file,
            sep="\t",
            quotechar='"',
            doublequote=True,
            quoting=1,  # csv.QUOTE_ALL
            na_values=["\\N", ""],  # Better null handling
            keep_default_na=True,
        )

        # Use to_sql with method='multi' for better performance and transaction safety
        df.to_sql(
            table_name,
            engine,
            method=psql_insert_copy,
            if_exists="replace",
            index=False,
        )

        print(f"Table {table_name} created and populated successfully.")


if __name__ == "__main__":
    load_csv_to_database()
