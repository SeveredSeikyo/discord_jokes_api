import sqlite3
import pandas as pd

# Establish connection
con = sqlite3.connect("./jokes.db")
cursor = con.cursor()

# ✅ Create table if not exists
cursor.execute('''
    CREATE TABLE IF NOT EXISTS jokes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        joke TEXT NOT NULL UNIQUE
    );
''')
print("Checked/Created table: jokes")

# Read cleaned jokes from CSV
fetched_jokes = pd.read_csv("./cleaned_jokes.csv")

# Extract 'joke' column (assumes column is named '0' if no headers)
fetched_jokes_arr = fetched_jokes['0']

# Insert statement with IGNORE to skip duplicates
insert_stmt = 'INSERT OR IGNORE INTO jokes(joke) VALUES (?)'

# Insert fetched jokes
print("\nInserting Fetched Jokes...")
fetched_inserted_count = 0
fetched_skipped_count = 0

for j in range(len(fetched_jokes_arr)):
    joke_text = fetched_jokes_arr[j]
    try:
        cursor.execute(insert_stmt, (joke_text,))
        if cursor.rowcount > 0:
            fetched_inserted_count += 1
        else:
            fetched_skipped_count += 1
    except sqlite3.Error as e:
        print(f"Error inserting Fetched joke '{joke_text[:50]}...': {e}")

print(f"Finished inserting Fetched Jokes. Inserted: {fetched_inserted_count}, Skipped (duplicates): {fetched_skipped_count}")

# Commit changes
con.commit()
print("\nAll insertions attempted and changes committed to the database.")

# Final count of unique jokes
cursor.execute("SELECT COUNT(*) FROM jokes;")
total_unique_jokes = cursor.fetchone()[0]
print(f"Total unique jokes in database: {total_unique_jokes}")

# Close connection
con.close()
print("Database connection closed successfully!")
