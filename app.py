# Import necessary modules
from flask import Flask, jsonify
import json
import os
import sqlite3
import threading

# Create an instance of the Flask application
app = Flask(__name__)

COUNTER_FILE = './counter.json'

DATABASE_FILE = './jokes.db'

# Create a lock for thread-safe access to the counter file.
file_lock = threading.Lock()

# --- Database Helper Functions ---

def get_db_connection():
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row # This allows accessing columns by name
    return conn

def initialize_database():
    
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Create the jokes table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS jokes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                joke TEXT NOT NULL
            )
        ''')
        conn.commit()
        print(f"Table 'jokes' ensured in '{DATABASE_FILE}'.")
        print("No initial jokes were inserted. Please add jokes to the 'jokes' table manually.")

    except sqlite3.Error as e:
        print(f"Database initialization error: {e}")
        conn.rollback() # Rollback changes if an error occurs
    finally:
        conn.close()

def get_total_jokes_count():
    """
    Returns the total number of jokes in the database.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT COUNT(*) FROM jokes')
        count = cursor.fetchone()[0]
        return count
    except sqlite3.Error as e:
        print(f"Error getting total joke count: {e}")
        return 0 # Return 0 if there's a database error
    finally:
        conn.close()

# --- Counter File Operations ---

def read_counter():
    """
    Reads the 'current_joke_id' from the COUNTER_FILE.
    If the file doesn't exist or is invalid, it initializes it with 1.
    Ensures thread-safe access using a lock.
    """
    with file_lock:
        if not os.path.exists(COUNTER_FILE):
            print(f"'{COUNTER_FILE}' not found. Creating with initial counter: 1.")
            initial_data = {"current_joke_id": 1}
            try:
                with open(COUNTER_FILE, 'w', encoding='utf-8') as f:
                    json.dump(initial_data, f, indent=4)
                return 1
            except IOError as e:
                print(f"Error creating '{COUNTER_FILE}': {e}")
                return 1
        
        try:
            with open(COUNTER_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                counter = data.get("current_joke_id", 1)
                # Ensure the counter is at least 1
                if counter < 1:
                    print(f"Invalid counter value ({counter}) in '{COUNTER_FILE}'. Resetting to 1.")
                    counter = 1
                return counter
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error reading or decoding JSON from '{COUNTER_FILE}': {e}. Resetting counter to 1.")
            write_counter(1)
            return 1
        except Exception as e:
            print(f"An unexpected error occurred while reading '{COUNTER_FILE}': {e}. Resetting counter to 1.")
            write_counter(1)
            return 1

def write_counter(new_value):
    """
    Writes the new 'current_joke_id' to the COUNTER_FILE.
    Ensures thread-safe access using a lock.
    """
    with file_lock:
        try:
            with open(COUNTER_FILE, 'w', encoding='utf-8') as f:
                json.dump({"current_joke_id": new_value}, f, indent=4)
        except IOError as e:
            print(f"Error writing to '{COUNTER_FILE}': {e}")
        except Exception as e:
            print(f"An unexpected error occurred while writing to '{COUNTER_FILE}': {e}")

# --- API Endpoint ---

@app.route('/joke', methods=['GET'])
def get_joke():
    """
    Returns a joke based on the 'current_joke_id' from COUNTER_FILE,
    fetching it from the SQLite database.
    Increments the counter and saves it back to the file for the next request.
    The jokes loop sequentially.
    """
    current_joke_id = read_counter()
    total_jokes = get_total_jokes_count()

    if total_jokes == 0:
        return jsonify({"error": "No jokes found in the database. Please add jokes to the 'jokes' table."}), 500

    # Ensure current_joke_id is within valid bounds (1 to total_jokes)
    # If the counter is greater than total_jokes, it means we've gone past the last joke
    # or jokes were deleted. Reset to 1.
    if not (1 <= current_joke_id <= total_jokes):
        print(f"Counter out of bounds ({current_joke_id}). Resetting to 1.")
        current_joke_id = 1
        write_counter(current_joke_id) # Immediately write the reset counter

    conn = get_db_connection()
    cursor = conn.cursor()
    selected_joke = None

    try:
        # Fetch the joke using the current_joke_id
        cursor.execute('SELECT id, joke FROM jokes WHERE id = ?', (current_joke_id,))
        joke_row = cursor.fetchone()

        if joke_row:
            selected_joke = dict(joke_row) # Convert Row object to dictionary
        else:
            # This case might happen if a joke was deleted or counter is out of sync
            print(f"Joke with ID {current_joke_id} not found. Attempting to find first joke.")
            # Try to fetch the first joke if the current ID is invalid
            cursor.execute('SELECT id, joke FROM jokes ORDER BY id ASC LIMIT 1')
            first_joke_row = cursor.fetchone()
            if first_joke_row:
                selected_joke = dict(first_joke_row)
                current_joke_id = selected_joke['id'] # Update current_joke_id to the first joke's ID
                print(f"Resetting joke ID to first available: {current_joke_id}.")
            else:
                return jsonify({"error": "No jokes available in the database."}), 500

    except sqlite3.Error as e:
        print(f"Database query error: {e}")
        return jsonify({"error": "Failed to fetch joke from database."}), 500
    finally:
        conn.close()

    # Calculate the next joke ID for the next request
    # If current_joke_id is the last joke, loop back to the first joke's ID
    if current_joke_id == total_jokes:
        next_joke_id = 1
    else:
        next_joke_id = current_joke_id + 1

    # Write the updated counter back to the file
    write_counter(next_joke_id)

    # Return the selected joke as a JSON response
    return jsonify({"joke_id": selected_joke['id'], "joke": selected_joke['joke']})

# This block ensures that the Flask application runs only when the script is executed directly
if __name__ == '__main__':
    # Initialize the database (create table if it doesn't exist, but don't populate)
    initialize_database()
    # Run the Flask application in debug mode.
    app.run(debug=True)
