import pandas as pd
import psycopg2
import psycopg2.extras as extras
import numpy as np
import sys
from configparser import ConfigParser
from io import StringIO
import warnings

# --- START: FILE PATH CONFIGURATION ---
# Please update these paths to point to your files.
# Use forward slashes (/) or double backslashes (\\) for paths.
# Example Windows: "C:\\Users\\YourUser\\Documents\\database.ini"
# Example macOS/Linux: "/home/youruser/documents/database.ini"

# Path to your database.ini file
CONFIG_FILE_PATH = '/opt/spark/apps/producer/database.ini'

# Paths to your CSV data files
CUSTOMERS_CSV_PATH = '/opt/spark/raw_data/sql_server/customers.csv'
POLICIES_CSV_PATH = '/opt/spark/raw_data/sql_server/policies.csv'
CLAIMS_CSV_PATH = '/opt/spark/raw_data/sql_server/claims.csv'

# --- END: FILE PATH CONFIGURATION ---


def load_config(filename, section='postgresql'):
    """
    Parses the specified.ini file to retrieve connection parameters.
    """
    parser = ConfigParser()
    if not parser.read(filename):
        raise Exception(f'File {filename} not found.')
    
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file.')
    
    return config

def clean_customers_data(df):
    """
    Cleans and transforms the customers DataFrame to match the SQL schema.
    """
    print("Cleaning customers.csv...")
    # Rename columns to match the target SQL table
    df = df.rename(columns={
        'customer_id': 'customer_id',
        'date_of_birth': 'date_of_birth',
        'borough': 'borough',
        'neighborhood': 'neighborhood',
        'zip_code': 'zip_code',
        'name': 'name'
    })

    # Clean and type-cast columns
    df['customer_id'] = pd.to_numeric(df['customer_id'], errors='coerce').astype('Int64')
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format='%d-%m-%Y', errors='coerce')

    df['zip_code'] = df['zip_code'].str.strip()
    # Use .str.replace() to remove commas from anywhere in the string.
    # .str.strip() only removes characters from the beginning or end.
    df['name'] = df['name'].astype(str).str.strip().str.strip('"').str.replace(",", "", regex=False)
    df['borough'] = df['borough'].str.strip()
    df['neighborhood'] = df['neighborhood'].str.strip()
    
    # Drop rows where the primary key (customer_id) is missing
    df = df.dropna(subset=['customer_id'])
    
    return df

def clean_policies_data(df):
    """
    Cleans and transforms the policies DataFrame to match the SQL schema.
    """
    print("Cleaning policies.csv...")
    # Rename columns
    df = df.rename(columns={
        'POLICY_NO': 'policy_no',
        'CUST_ID': 'cust_id',
        'POLICYTYPE': 'policytype',
        'POL_ISSUE_DATE': 'pol_issue_date',
        'POL_EFF_DATE': 'pol_eff_date',
        'POL_EXPIRY_DATE': 'pol_expiry_date',
        'MAKE': 'make',
        'MODEL': 'model',
        'MODEL_YEAR': 'model_year',
        'CHASSIS_NO': 'chassis_no',
        'USE_OF_VEHICLE': 'use_of_vehicle',
        'PRODUCT': 'product',
        'SUM_INSURED': 'sum_insured',
        'PREMIUM': 'premium',
        'DEDUCTABLE': 'deductable'
    })

    # Clean and type-cast columns
    df['policy_no'] = df['policy_no'].astype(str).str.strip()
    # Convert cust_id to a number, then a nullable integer to safely drop decimals
    # like '.0', and finally back to a string to match the database schema (VARCHAR).
    # This handles values like 999.0 -> 999 -> "999".
    df['cust_id'] = pd.to_numeric(df['cust_id'], errors='coerce')
    df['cust_id'] = df['cust_id'].astype('Int64').astype(str).replace('<NA>', np.nan)

    df['pol_issue_date'] = pd.to_datetime(df['pol_issue_date'], errors='coerce')
    df['pol_eff_date'] = pd.to_datetime(df['pol_eff_date'], errors='coerce')
    df['pol_expiry_date'] = pd.to_datetime(df['pol_expiry_date'], errors='coerce')
    
    # Strip extraneous characters and whitespace
    df['model'] = df['model'].str.strip()
    df['chassis_no'] = df['chassis_no'].astype(str).str.strip('.')
    
    df['model_year'] = pd.to_numeric(df['model_year'], errors='coerce').astype('Int64')
    df['product'] = df['product'].replace('NOT CLASSIFIED', np.nan).str.strip()
    
    # Convert financial columns
    df['sum_insured'] = pd.to_numeric(df['sum_insured'], errors='coerce')
    df['premium'] = pd.to_numeric(df['premium'], errors='coerce')
    df['deductable'] = pd.to_numeric(df['deductable'], errors='coerce').astype('Int64')

    # Drop rows where the primary key (policy_no) is missing
    df = df.dropna(subset=['policy_no'])

    return df

def clean_claims_data(df):
    """
    Cleans and transforms the claims DataFrame to match the SQL schema.
    """
    print("Cleaning claims.csv...")
    # Rename columns to be SQL-friendly
    df = df.rename(columns={
        'claim_no': 'claim_no',
        'policy_no': 'policy_no',
        'claim_date': 'claim_date', # Matches schema
        'months_as_customer': 'months_as_customer', # Matches schema
        'injury': 'injury', # Matches schema
        'property': 'property', # Matches schema
        'vehicle': 'vehicle', # Matches schema
        'total': 'total', # Matches schema
        'collision_type': 'collision_type', # Matches schema
        'number_of_vehicles_involved': 'number_of_vehicles_involved',
        'age': 'driver_age',
        'number_of_witnesses': 'number_of_witnesses',
        'date': 'incident_date',
        'hour': 'incident_hour',
        'type': 'incident_type',
        'severity': 'incident_severity'
    })
    # Standardize missing values (replace 'null' string with NaN)
    df['collision_type'] = df['collision_type'].replace('null', np.nan).str.strip()

    # Clean and type-cast columns
    df['claim_no'] = df['claim_no'].astype(str).str.strip()
    df['policy_no'] = df['policy_no'].astype(str).str.strip()
    df['claim_date'] = pd.to_datetime(df['claim_date'], errors='coerce')
    df['incident_date'] = pd.to_datetime(df['incident_date'], errors='coerce')
    
    # Parse the non-standard DD-MM-YYYY date format
    df['license_issue_date'] = pd.to_datetime(df['license_issue_date'], format='%d-%m-%Y', errors='coerce')
    
    # Map string 'true'/'false' to a nullable boolean
    bool_map = {'true': True, 'false': False}
    df['suspicious_activity'] = df['suspicious_activity'].astype(str).str.lower().map(bool_map).astype('boolean')

    # Convert numeric columns to nullable integers
    int_cols = ['months_as_customer', 'injury', 'property', 'vehicle', 'total', 
                'number_of_vehicles_involved', 'driver_age', 'incident_hour', 'number_of_witnesses']
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        
    # Clean text fields
    text_cols = ['insured_relationship', 'incident_type', 'incident_severity']
    for col in text_cols:
        df[col] = df[col].str.strip()

    # Drop rows where the primary key (claim_no) is missing
    df = df.dropna(subset=['claim_no'])

    return df

def bulk_insert_idempotent(conn, df, table_name, pk_column):
    """
    Performs a high-performance, idempotent bulk insert of a DataFrame
    using psycopg2.extras.execute_values.
    
    Rows with conflicting Primary Keys will be silently ignored.
    """
    cursor = conn.cursor()
    
    # Prepare the data for insertion
    # Replace pandas <NA> and NaT with None (which SQL understands as NULL)
    df_cleaned_for_sql = df.replace({pd.NaT: None, pd.NA: None})
    df_tuples = [tuple(x) for x in df_cleaned_for_sql.to_numpy()]
    
    # Get column names, quoting them for safety
    cols = ','.join([f'"{c}"' for c in df_cleaned_for_sql.columns])
    
    # Construct the idempotent INSERT query
    query = f"""
        INSERT INTO {table_name} ({cols})
        VALUES %s
        ON CONFLICT ({pk_column}) DO NOTHING
    """
    
    try:
        extras.execute_values(cursor, query, df_tuples)
        print(f"Successfully loaded or skipped data into {table_name}.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error loading data into {table_name}: {error}")
        # Re-raise the error to trigger the transaction rollback
        raise error
    finally:
        cursor.close()

def main():
    """
    Main ETL orchestration function.
    """
    conn = None
    # Suppress pandas warnings about nullable types
    warnings.simplefilter(action='ignore', category=FutureWarning)
    
    try:
        # --- CONNECT ---
        print("Connecting to the PostgreSQL database...")
        # Use the global path for the config file
        config = load_config(CONFIG_FILE_PATH)
        conn = psycopg2.connect(**config)
        
        # --- EXTRACT & TRANSFORM ---
        print("Reading and cleaning source files...")
        
        # Process Customers - use global path
        # Explicitly read 'zip_code' and 'customer_id' as strings to prevent
        # automatic conversion to numbers, which can drop leading zeros or
        # create floats.
        df_customers = pd.read_csv(
            CUSTOMERS_CSV_PATH, 
            na_values=['null'], 
            dtype={'zip_code': str})
        df_customers_clean = clean_customers_data(df_customers)
        
        # Process Policies - use global path
        df_policies = pd.read_csv(POLICIES_CSV_PATH, na_values=['null'])
        df_policies_clean = clean_policies_data(df_policies)
        
        # Process Claims - use global path
        df_claims = pd.read_csv(CLAIMS_CSV_PATH, na_values=['null'])
        df_claims_clean = clean_claims_data(df_claims)
        
        print("All files cleaned. Starting database load.")
        
        # --- LOAD (in dependency order) ---
        
        # 1. Load Customers
        bulk_insert_idempotent(conn, df_customers_clean, 'demo.customer', 'customer_id')
        
        # 2. Load Policies
        bulk_insert_idempotent(conn, df_policies_clean, 'demo.policy', 'policy_no')
        
        # 3. Load Claims
        bulk_insert_idempotent(conn, df_claims_clean, 'demo.claim', 'claim_no')
        
        # --- COMMIT ---
        # If all loads are successful, commit the transaction
        conn.commit()
        print("\nETL process complete. All data successfully committed.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"\n--- FATAL ERROR --- \n{error}")
        if conn:
            # Roll back all changes if any part of the process fails
            conn.rollback()
            print("Transaction has been rolled back. Database is in its original state.")
            
    finally:
        # Ensure the connection is always closed
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()