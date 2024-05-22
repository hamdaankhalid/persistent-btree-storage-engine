import sqlite3
import random
import string

# Define the schema
schema = {
    "id": "INTEGER",
    "name": "TEXT",
    "age": "INTEGER"
}

# Create a connection to the SQLite database
# If the database does not exist, it will be created
conn = sqlite3.connect('./sample.db')

# Create a cursor object
c = conn.cursor()

# Create table
table_name = "RandomData"
columns = ', '.join(f'{k} {v}' for k, v in schema.items())
c.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")

# Insert random data
for _ in range(100000):  # Insert 100 rows
    data = []
    for v in schema.values():
        if v == "INTEGER":
            data.append(random.randint(6, 100))
        elif v == "TEXT":
            data.append(''.join(random.choices(string.ascii_uppercase + string.digits, k=5)))
        else:
            data.append(None)  # Add more types as needed

    placeholders = ', '.join('?' for _ in data)
    c.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", data)

# Commit the changes and close the connection
conn.commit()
conn.close()