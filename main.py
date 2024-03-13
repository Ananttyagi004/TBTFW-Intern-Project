import psycopg2
import csv

# PostgreSQL connection information
DB_NAME = "Stocks"
DB_USER = "postgres"
DB_PASSWORD = "anant"
DB_HOST = "localhost"
DB_PORT = "5432"

# CSV file path
CSV_FILE = "TATAMOTORS.csv"
TABLE_NAME = "TATA"
# Data types for each column
COLUMN_DATA_TYPES = ['DATE', 'NUMERIC', 'NUMERIC', 'NUMERIC', 'NUMERIC', 'NUMERIC', 'NUMERIC']

# Function to create PostgreSQL table schema
def create_table_schema(cursor, csv_header, column_data_types):
    cursor.execute(f'CREATE TABLE IF NOT EXISTS {TABLE_NAME} (date DATE,open NUMERIC,high NUMERIC,low NUMERIC,close NUMERIC,adj_close NUMERIC,volume BIGINT)')

# Function to insert data into PostgreSQL table
def insert_data(cursor, csv_data):
    for row in csv_data:
        placeholders = ', '.join(['%s'] * len(row))
        cursor.execute(f'INSERT INTO {TABLE_NAME} VALUES ({placeholders})', row)

# Function to read CSV file
def read_csv(file_path):
    with open(file_path, newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        header = next(csv_reader)
        data = [row for row in csv_reader]
    return header, data

# Main function to connect to PostgreSQL and execute operations
def main():
    conn = None
    cur = None
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        
        # Create a cursor object
        cur = conn.cursor()
        
        # Read CSV file
        csv_header, csv_data = read_csv(CSV_FILE)
        
        # Create table schema with specified data types
        create_table_schema(cur, csv_header, COLUMN_DATA_TYPES)
        
        # Insert data into table
        insert_data(cur, csv_data)
        
        # Commit the transaction
        conn.commit()
        
        print("Data inserted successfully!")
    
    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
    
    finally:
        # Close cursor and connection
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
