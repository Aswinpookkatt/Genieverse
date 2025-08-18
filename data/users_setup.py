import duckdb
import hashlib

# Connect to DuckDB & initialize user table
conn = duckdb.connect('./data/users_db.duckdb')
 
# Create users table if not exists
conn.execute("""
CREATE TABLE IF NOT EXISTS users (
    username VARCHAR PRIMARY KEY,
    password_hash VARCHAR
)
""")


# Helper function to hash password
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

# Function to add a sample user (only for demo; in production, use registration logic)
def add_sample_user():
    username = 'Aswin'
    password = 'root'
    password_hash = hash_password(password)
    # Check if user exists before inserting
    user = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    if not user:
        result = conn.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)", (username, password_hash))
    print(result)

add_sample_user()