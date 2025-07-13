import duckdb

#print(duckdb.__version__)
con = duckdb.connect('./data/mydb.duckdb')  # Or use the path from your config.toml


con.execute("""
    CREATE TABLE customer_dim AS
    SELECT * FROM read_csv_auto('./data/customer_dim.csv', encoding='cp1252');
""")
con.execute("""
    CREATE TABLE fact_table AS
    SELECT * FROM read_csv_auto('./data/fact_table.csv',encoding='utf-8');
""")
con.execute("""
    CREATE TABLE item_dim AS
    SELECT * FROM read_csv_auto('./data/item_dim.csv',encoding='cp1252');
""")
con.execute("""
    CREATE TABLE store_dim AS
    SELECT * FROM read_csv_auto('./data/store_dim.csv', encoding='cp1252');
""")
con.execute("""
    CREATE TABLE time_dim AS
    SELECT * FROM read_csv_auto('./data/time_dim.csv', encoding='utf-8');
""")
con.execute("""
    CREATE TABLE Trans_dim AS
    SELECT * FROM read_csv_auto('./data/Trans_dim.csv', encoding='utf-8');
""")


tables = con.execute("SHOW TABLES").fetchdf()
print(tables)

# result = con.execute("SELECT * FROM customer_dim ").fetchdf()
# print(result)