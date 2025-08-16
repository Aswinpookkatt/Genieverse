import duckdb

#print(duckdb.__version__)
con = duckdb.connect('./data/amazon.duckdb')  # Or use the path from your config.toml


con.execute("""
CREATE TABLE Sales AS
SELECT
    regexp_replace(actual_price, ',', '', 'g')::DOUBLE AS actual_price,
    regexp_replace(discounted_price, ',', '', 'g')::DOUBLE AS discounted_price,
    regexp_replace(rating_count, ',', '', 'g')::DOUBLE AS rating_count,
    *
EXCLUDE(actual_price, discounted_price,rating_count)
FROM read_csv_auto('./data/amazon.csv', encoding='UTF-8');

""")


tables = con.execute("SHOW TABLES").fetchdf()
print(tables)

result = con.execute("SELECT * FROM Sales ").fetchdf()
print(result)
