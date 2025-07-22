import sqlite3
import pandas as pd

con = sqlite3.connect('./jokes.db')
cur = con.cursor()

try:
    # This is a better way to structure your code.
    # Put the main logic in the 'try' block.
    # Fetch all jokes from the source table
    res = cur.execute('select joke from uncleaned_jokes;')
    joke_array = res.fetchall() # This returns a list of tuples: [('joke1',), ('joke2',), ...]

    insert_stmt = 'INSERT OR IGNORE INTO jokes(joke) VALUES (?)'

    # Use executemany for efficiency
    # This is much faster for a large number of rows
    # However, it expects an iterable of iterables, which is exactly what joke_array is.
    print(f"Transferring {len(joke_array)} jokes from uncleaned_jokes to jokes table...")
    cur.executemany(insert_stmt, joke_array)
    
    joke_inserted_count = cur.rowcount
    
    # You can't get the skipped count directly from executemany, but you can infer it
    # by checking the final count in the destination table.

    print(f"Finished inserting Jokes. {joke_inserted_count} rows affected.")
    
    # Commit the changes
    con.commit()
    
    # Verify the count in the new table
    cur.execute("SELECT COUNT(*) FROM jokes;")
    final_count = cur.fetchone()[0]
    print(f"Final count of jokes in the 'jokes' table: {final_count}")

except sqlite3.Error as e:
    print(f"An SQLite error occurred: {e}")
    # Rollback changes if an error occurred
    con.rollback()

finally:
    # This block is for cleanup that should always run
    if con:
        con.close()
    print("Database connection closed.")