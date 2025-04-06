from dotenv import load_dotenv
import os
import glob
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()

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
            escapechar='\\',
            na_values=['N', 'NA', 'NULL', ''],  # Better null handling
            keep_default_na=True
        )

        # Clean string columns (replace newlines/tabs with spaces)
        for column in df.select_dtypes(include=['object']).columns:
            df[column] = df[column].str.replace(r'[\n\t]', ' ', regex=True).str.strip()

        # Use to_sql with method='multi' for better performance and transaction safety
        df.to_sql(
            table_name,
            engine,
            if_exists='replace',
            index=False,
            method='multi',  # Uses multiple INSERT statements
            chunksize=1000    # Process in chunks to avoid memory issues
        )

        print(f"Table {table_name} created and populated successfully.")

if __name__ == "__main__":
    load_csv_to_database()
