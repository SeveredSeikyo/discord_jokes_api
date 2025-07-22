import sqlite3
import pandas as pd

# Establish connection
con = sqlite3.connect("./jokes.db")
cursor = con.cursor() # It's good practice to use a cursor for executing commands

fetched_jokes = pd.read_csv("./cleaned_jokes.csv")

# Extract 'joke' columns as pandas Series
fetched_jokes_arr = fetched_jokes['0']

# Define the insert statement with IGNORE
# This will skip any joke that already exists due to the UNIQUE constraint
insert_stmt = 'INSERT OR IGNORE INTO uncleaned_jokes(joke) VALUES (?)'

# Insert Fetched jokes
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

# Don't forget to commit your changes!
con.commit()
print("\nAll insertions attempted and changes committed to the database.")

# Optional: Get final count of unique jokes
cursor.execute("SELECT COUNT(*) FROM uncleaned_jokes;")
total_unique_jokes = cursor.fetchone()[0]
print(f"Total unique jokes in database: {total_unique_jokes}")

con.close()
print("Database connection closed successfully!")