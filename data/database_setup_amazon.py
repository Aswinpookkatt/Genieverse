import duckdb

#print(duckdb.__version__)
con = duckdb.connect('./data/amazon.duckdb')  # Or use the path from your config.toml


con.execute("""
    CREATE TABLE Sales AS
    SELECT * FROM read_csv_auto('./data/amazon.csv', encoding='UTF-8');
""")


tables = con.execute("SHOW TABLES").fetchdf()
print(tables)

result = con.execute("SELECT * FROM Sales ").fetchdf()
print(result)