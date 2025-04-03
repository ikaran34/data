from prefect import flow, task
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Database connection settings (Modify these as per your DB)
DB_CONFIG = {
    "dbname": "your_db_name",
    "user": "your_username",
    "password": "your_password",
    "host": "localhost",  # Change if using a cloud DB
    "port": "5432",       # Default PostgreSQL port
}

# Extract Task
@task
def extract_data(file_path: str) -> pd.DataFrame:
    """Extracts data from a CSV file."""
    df = pd.read_csv(file_path)
    print(f"Extracted {len(df)} records from {file_path}")
    return df

# Transform Task
@task
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transforms data (e.g., removing nulls, formatting columns)."""
    df.dropna(inplace=True)  # Remove null values
    df['processed_at'] = pd.Timestamp.now()  # Add a processing timestamp
    print(f"Transformed data: {df.shape[0]} records remain")
    return df

# Load Task
@task
def load_data(df: pd.DataFrame, table_name: str):
    """Loads the transformed data into a PostgreSQL table."""
    engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"Loaded {len(df)} records into {table_name}")

# Prefect Flow
@flow
def etl_flow():
    """Main ETL Flow"""
    file_path = "customer_data.csv"  # Ensure this file exists in your directory
    table_name = "customer_data"

    data = extract_data(file_path)
    cleaned_data = transform_data(data)
    load_data(cleaned_data, table_name)

if __name__ == "__main__":
    etl_flow()

