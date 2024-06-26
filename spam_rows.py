import sqlite3
import random
import string

# Data Seeding Program to create something meaningful to test against.

# Define the schema
schema = {
    "id": "INTEGER",
    "name": "TEXT",
    "age": "INTEGER"
}

# Create a connection to the SQLite database
# If the database does not exist, it will be created
conn = sqlite3.connect('./sample.db')

print("Opened database successfully")

# Create a cursor object
c = conn.cursor()

# Create table
table_name = "RandomData"
columns = ', '.join(f'{k} {v}' for k, v in schema.items())
c.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
c.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_name ON {table_name} (name)")

# Insert random data
for _ in range(10000):
    data = []
    for v in schema.values():
        if v == "INTEGER":
            data.append(random.randint(10, 100))
        elif v == "TEXT":
            data.append(''.join(random.choices(string.ascii_uppercase + string.digits, k=5)))
        else:
            data.append(None)  # Add more types as needed

    placeholders = ', '.join('?' for _ in data)
    c.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", data)

# Commit the changes and close the connection
conn.commit()
conn.close()

print("Seeded database successfully")