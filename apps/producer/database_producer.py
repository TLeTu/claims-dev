import sys
import warnings
import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras as extras
from configparser import ConfigParser

# --- Configuration ---
CONFIG_FILE = '/opt/spark/apps/producer/database.ini'
PATH_CUSTOMERS = '/opt/spark/raw_data/sql_server/customers.csv'
PATH_POLICIES = '/opt/spark/raw_data/sql_server/policies.csv'
PATH_CLAIMS = '/opt/spark/raw_data/sql_server/claims.csv'

def get_db_config(filename, section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)
    if parser.has_section(section):
        params = parser.items(section)
        return {param[0]: param[1] for param in params}
    raise Exception(f'Section {section} not found in {filename}')

# --- Transformation Logic ---

def clean_customers(df):
    print("Transforming customers data...")
    df = df.rename(columns={
        'customer_id': 'customer_id', 'date_of_birth': 'date_of_birth',
        'borough': 'borough', 'neighborhood': 'neighborhood',
        'zip_code': 'zip_code', 'name': 'name'
    })

    df['customer_id'] = pd.to_numeric(df['customer_id'], errors='coerce').astype('Int64')
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format='%d-%m-%Y', errors='coerce')
    df['name'] = df['name'].astype(str).str.strip().str.strip('"').str.replace(",", "", regex=False)
    
    return df.dropna(subset=['customer_id'])

def clean_policies(df):
    print("Transforming policies data...")
    df = df.rename(columns={
        'POLICY_NO': 'policy_no', 'CUST_ID': 'cust_id', 'POLICYTYPE': 'policytype',
        'POL_ISSUE_DATE': 'pol_issue_date', 'POL_EFF_DATE': 'pol_eff_date',
        'POL_EXPIRY_DATE': 'pol_expiry_date', 'MAKE': 'make', 'MODEL': 'model',
        'MODEL_YEAR': 'model_year', 'CHASSIS_NO': 'chassis_no', 
        'USE_OF_VEHICLE': 'use_of_vehicle', 'PRODUCT': 'product',
        'SUM_INSURED': 'sum_insured', 'PREMIUM': 'premium', 'DEDUCTABLE': 'deductable'
    })

    df['cust_id'] = pd.to_numeric(df['cust_id'], errors='coerce').astype('Int64').astype(str).replace('<NA>', np.nan)
    df['pol_issue_date'] = pd.to_datetime(df['pol_issue_date'], errors='coerce')
    df['pol_eff_date'] = pd.to_datetime(df['pol_eff_date'], errors='coerce')
    df['pol_expiry_date'] = pd.to_datetime(df['pol_expiry_date'], errors='coerce')
    df['model_year'] = pd.to_numeric(df['model_year'], errors='coerce').astype('Int64')
    
    return df.dropna(subset=['policy_no'])

def clean_claims(df):
    print("Transforming claims data...")
    df = df.rename(columns={
        'age': 'driver_age', 'date': 'incident_date', 
        'hour': 'incident_hour', 'type': 'incident_type', 'severity': 'incident_severity'
    })
    
    df['collision_type'] = df['collision_type'].replace('null', np.nan).str.strip()
    df['claim_date'] = pd.to_datetime(df['claim_date'], errors='coerce')
    df['incident_date'] = pd.to_datetime(df['incident_date'], errors='coerce')
    df['license_issue_date'] = pd.to_datetime(df['license_issue_date'], format='%d-%m-%Y', errors='coerce')
    
    bool_map = {'true': True, 'false': False}
    df['suspicious_activity'] = df['suspicious_activity'].astype(str).str.lower().map(bool_map).astype('boolean')

    int_cols = ['months_as_customer', 'injury', 'property', 'vehicle', 'total', 
                'number_of_vehicles_involved', 'driver_age', 'incident_hour', 'number_of_witnesses']
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    return df.dropna(subset=['claim_no'])

# --- Database Operations ---

def bulk_upsert(conn, df, table, pk):
    cursor = conn.cursor()
    df_clean = df.replace({pd.NaT: None, pd.NA: None})
    data_tuples = [tuple(x) for x in df_clean.to_numpy()]
    cols = ','.join([f'"{c}"' for c in df_clean.columns])
    
    query = f"INSERT INTO {table} ({cols}) VALUES %s ON CONFLICT ({pk}) DO NOTHING"
    
    try:
        extras.execute_values(cursor, query, data_tuples)
        print(f"Synced {table}.")
    except Exception as e:
        print(f"Error syncing {table}: {e}")
        raise e
    finally:
        cursor.close()

def main():
    warnings.simplefilter(action='ignore', category=FutureWarning)
    conn = None
    
    try:
        print("Connecting to database...")
        config = get_db_config(CONFIG_FILE)
        conn = psycopg2.connect(**config)
        
        # Extract
        print("Reading source files...")
        df_cust = pd.read_csv(PATH_CUSTOMERS, na_values=['null'], dtype={'zip_code': str})
        df_pol = pd.read_csv(PATH_POLICIES, na_values=['null'])
        df_claim = pd.read_csv(PATH_CLAIMS, na_values=['null'])
        
        # Transform & Load
        bulk_upsert(conn, clean_customers(df_cust), 'demo.customer', 'customer_id')
        bulk_upsert(conn, clean_policies(df_pol), 'demo.policy', 'policy_no')
        bulk_upsert(conn, clean_claims(df_claim), 'demo.claim', 'claim_no')
        
        conn.commit()
        print("Database load complete.")

    except Exception as e:
        print(f"Fatal Error: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    main()