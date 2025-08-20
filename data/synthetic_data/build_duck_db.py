import duckdb
import pandas as pd
import os

# === Change to script directory ===
os.chdir(os.path.dirname(__file__))

# === File paths (relative to script location) ===
csv_files = {
    "customers": "dataset/customers.csv",
    "products": "dataset/products.csv", 
    "orders": "dataset/orders.csv",
    "reviews": "dataset/reviews.csv"
}

def pandas_to_duckdb_type(dtype):
    """Convert pandas dtype to DuckDB SQL type"""
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "DOUBLE"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    elif pd.api.types.is_object_dtype(dtype):
        return "VARCHAR"
    else:
        return "VARCHAR"

def infer_schema_from_csv(file_path, table_name):
    """Infer schema from CSV file and return CREATE TABLE statement"""
    print(f"Analyzing schema for {table_name}...")
    
    # Read a sample to infer types
    df_sample = pd.read_csv(file_path, nrows=1000)
    
    # Try to convert date columns
    for col in df_sample.columns:
        if 'date' in col.lower() or 'time' in col.lower():
            try:
                df_sample[col] = pd.to_datetime(df_sample[col])
            except:
                pass
    
    # Build CREATE TABLE statement
    columns = []
    for col, dtype in df_sample.dtypes.items():
        sql_type = pandas_to_duckdb_type(dtype)
        
        # Add PRIMARY KEY constraint for ID columns
        if col.lower().endswith('_id') and col == df_sample.columns[0]:
            columns.append(f"    {col} {sql_type} PRIMARY KEY")
        else:
            columns.append(f"    {col} {sql_type}")
    
    create_statement = f"""CREATE TABLE {table_name} (
{',\n'.join(columns)}
)"""
    
    print(f"Schema for {table_name}:")
    for col, dtype in df_sample.dtypes.items():
        print(f"  {col}: {dtype} -> {pandas_to_duckdb_type(dtype)}")
    
    return create_statement

# Check if files exist
print("Checking for CSV files...")
for table_name, file_path in csv_files.items():
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found!")
        exit(1)
    else:
        print(f"✓ Found: {os.path.basename(file_path)}")

print("\nAnalyzing schemas...")
# === Create DuckDB database ===
db_path = "ecommerce.duckdb"  # Save to current folder (same as script)
con = duckdb.connect(db_path)

# Drop old tables if exist (in correct order to handle foreign keys)
print("\nDropping existing tables...")

# First, try to disable foreign key constraints temporarily
try:
    con.execute("PRAGMA foreign_keys = OFF")
    print("✓ Disabled foreign key constraints")
except:
    pass

# Drop in order that respects dependencies
drop_order = ["reviews", "orders", "products", "customers"]  # Child tables first
for table_name in drop_order:
    try:
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f"✓ Dropped {table_name}")
    except Exception as e:
        print(f"Note: {table_name} didn't exist or couldn't be dropped: {e}")

# Re-enable foreign key constraints
try:
    con.execute("PRAGMA foreign_keys = ON")
    print("✓ Re-enabled foreign key constraints")
except:
    pass

# === Create tables with inferred schema ===
print("\nCreating tables with inferred schemas...")
for table_name, file_path in csv_files.items():
    create_statement = infer_schema_from_csv(file_path, table_name)
    print(f"\nExecuting: {create_statement}")
    con.execute(create_statement)

# === Insert data from CSVs using read_csv_auto ===
print("\nInserting data...")
for table_name, file_path in csv_files.items():
    print(f"Loading data into {table_name}...")
    con.execute(f"INSERT INTO {table_name} SELECT * FROM read_csv_auto('{file_path}')")

# === Verify schema and data ===
print("\n" + "="*50)
print("DATABASE CREATION COMPLETE")
print("="*50)

tables = con.execute("SHOW TABLES").fetchall()
print(f"Tables created: {[table[0] for table in tables]}")

for table_name in csv_files.keys():
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"{table_name}: {count:,} records")

print(f"\nDatabase saved to: {db_path}")

# Show detailed schema for each table
print("\n" + "="*50)
print("TABLE SCHEMAS")
print("="*50)
for table_name in csv_files.keys():
    print(f"\n{table_name.upper()} TABLE:")
    schema = con.execute(f"DESCRIBE {table_name}").fetchall()
    for col_info in schema:
        print(f"  {col_info[0]}: {col_info[1]}")

con.close()
