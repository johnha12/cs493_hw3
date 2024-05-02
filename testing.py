import sqlite3

# Path to the SQLite database file
DB_FILE = 'local_database.db'

# Connect to the SQLite database
connection = sqlite3.connect(DB_FILE)
cursor = connection.cursor()

# Create tables with maximum length constraint for name and city fields
cursor.execute('''
    CREATE TABLE IF NOT EXISTS businesses (
        id INTEGER PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        street_address VARCHAR(100) NOT NULL,
        city VARCHAR(50) NOT NULL,
        state TEXT NOT NULL,
        zip_code INTEGER NOT NULL
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS reviews (
        id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        business_id INTEGER NOT NULL,
        stars INTEGER NOT NULL,
        review_text TEXT,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (business_id) REFERENCES businesses(id)
    )
''')

# Commit changes and close connection
connection.commit()
connection.close()
