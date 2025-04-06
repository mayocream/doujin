from dotenv import load_dotenv
import psycopg2
import os
import glob
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
import re

load_dotenv()

def load_csv_to_database():
    # Create connections
    engine = create_engine(os.getenv("DATABASE_URL"))
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    conn.autocommit = False
    cursor = conn.cursor()

    csv_files = glob.glob("data/doujinshi.org/*.csv")

    for csv_file in csv_files:
        table_name = os.path.basename(csv_file).replace(".csv", "")
        print(f"Processing {csv_file} to {table_name}...")

        # Read the CSV file with pandas, handling quoted fields properly
        df = pd.read_csv(
            csv_file,
            sep="\t",
            quotechar='"',  # Specify the quote character
            doublequote=True,  # Handle double quotes within quoted fields
            quoting=1,  # csv.QUOTE_ALL equivalent
            escapechar='\\',  # Handle escape characters
            lineterminator='\n'  # Explicit line terminator
        )

        # Clean up data - replace internal newlines with spaces in quoted fields
        for column in df.select_dtypes(include=['object']).columns:
            # Only process non-null values
            mask = df[column].notna()
            if mask.any():
                # Replace tab and newline chars with spaces in string values
                df.loc[mask, column] = df.loc[mask, column].astype(str).apply(
                    lambda x: re.sub(r'[\n\t]', ' ', x).strip()
                )

        # Create table structure with first row
        df.head(1).to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Table {table_name} created.")

        # Convert DataFrame to CSV in memory
        csv_buffer = StringIO()
        df.to_csv(
            csv_buffer,
            sep='\t',
            header=False,
            index=False,
            quoting=3,  # csv.QUOTE_NONE equivalent
            escapechar='\\',
            lineterminator='\n'
        )
        csv_buffer.seek(0)

        # Use COPY for all data
        cursor.copy_from(
            csv_buffer,
            table_name,
            columns=df.columns.tolist(),
            sep="\t",
            null="N"
        )

        conn.commit()
        print(f"Data copied to {table_name} successfully.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    load_csv_to_database()
