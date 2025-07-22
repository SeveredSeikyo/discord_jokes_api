import sqlite3
import pandas as pd

# Establish connection
con = sqlite3.connect("./jokes.db")
cursor = con.cursor() # It's good practice to use a cursor for executing commands

# Read CSV files into pandas DataFrames
reddit_jokes = pd.read_csv("./reddit_dadjokes.csv")
dad_jokes = pd.read_csv("./dad_jokes.csv")
fetched_jokes = pd.read_csv("./fetched_jokes.csv")

# Extract 'joke' columns as pandas Series
reddit_jokes_arr = reddit_jokes['joke']
dad_jokes_arr = dad_jokes['joke']
fetched_jokes_arr = fetched_jokes['0']

# Define the insert statement with IGNORE
# This will skip any joke that already exists due to the UNIQUE constraint
insert_stmt = 'INSERT OR IGNORE INTO uncleaned_jokes(joke) VALUES (?)'

# Insert Reddit jokes
print("Inserting Reddit Jokes...")
reddit_inserted_count = 0
reddit_skipped_count = 0
for i in range(len(reddit_jokes_arr)):
    joke_text = reddit_jokes_arr[i]
    try:
        cursor.execute(insert_stmt, (joke_text,))
        # Check if a row was actually inserted (rowcount will be 1 if inserted, 0 if ignored)
        if cursor.rowcount > 0:
            reddit_inserted_count += 1
        else:
            reddit_skipped_count += 1
    except sqlite3.Error as e:
        print(f"Error inserting Reddit joke '{joke_text[:50]}...': {e}")
        # You might want to log this or handle it differently
        # For a UNIQUE constraint, IGNORE should prevent this, but good for general errors

print(f"Finished inserting Reddit Jokes. Inserted: {reddit_inserted_count}, Skipped (duplicates): {reddit_skipped_count}")

# Insert Dad jokes
print("\nInserting Dad Jokes...")
dad_inserted_count = 0
dad_skipped_count = 0
for j in range(len(dad_jokes_arr)):
    joke_text = dad_jokes_arr[j]
    try:
        cursor.execute(insert_stmt, (joke_text,))
        if cursor.rowcount > 0:
            dad_inserted_count += 1
        else:
            dad_skipped_count += 1
    except sqlite3.Error as e:
        print(f"Error inserting Dad joke '{joke_text[:50]}...': {e}")

print(f"Finished inserting Dad Jokes. Inserted: {dad_inserted_count}, Skipped (duplicates): {dad_skipped_count}")

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