from flask import Flask, jsonify
import json
import os
import sqlite3
import threading

app = Flask(__name__)

COUNTER_FILE = './counter.json'
DATABASE_FILE = './jokes.db'
file_lock = threading.Lock()

def get_db_connection():
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def initialize_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS jokes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                joke TEXT NOT NULL UNIQUE
            )
        ''')
        conn.commit()
        print(f"Table 'jokes' ensured in '{DATABASE_FILE}'.")
    except sqlite3.Error as e:
        print(f"Database initialization error: {e}")
        conn.rollback()
    finally:
        conn.close()

def get_total_jokes_count():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT COUNT(*) FROM jokes')
        return cursor.fetchone()[0]
    except sqlite3.Error as e:
        print(f"Error getting joke count: {e}")
        return 0
    finally:
        conn.close()

def read_counter():
    with file_lock:
        if not os.path.exists(COUNTER_FILE):
            try:
                with open(COUNTER_FILE, 'w', encoding='utf-8') as f:
                    json.dump({"current_joke_id": 1}, f, indent=4)
                return 1
            except IOError:
                return 1
        try:
            with open(COUNTER_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return max(1, data.get("current_joke_id", 1))
        except:
            write_counter(1)
            return 1

def write_counter(new_value):
    with file_lock:
        try:
            with open(COUNTER_FILE, 'w', encoding='utf-8') as f:
                json.dump({"current_joke_id": new_value}, f, indent=4)
        except:
            pass

@app.route('/joke', methods=['GET'])
def get_joke():
    current_joke_id = read_counter()
    total_jokes = get_total_jokes_count()

    if total_jokes == 0:
        return jsonify({"error": "No jokes found."}), 500

    if not (1 <= current_joke_id <= total_jokes):
        current_joke_id = 1
        write_counter(current_joke_id)

    conn = get_db_connection()
    cursor = conn.cursor()
    selected_joke = None

    try:
        cursor.execute('SELECT id, joke FROM jokes WHERE id = ?', (current_joke_id,))
        row = cursor.fetchone()

        if row:
            selected_joke = dict(row)
        else:
            cursor.execute('SELECT id, joke FROM jokes ORDER BY id ASC LIMIT 1')
            row = cursor.fetchone()
            if row:
                selected_joke = dict(row)
                current_joke_id = selected_joke['id']
    except:
        return jsonify({"error": "Failed to fetch joke."}), 500
    finally:
        conn.close()

    next_id = 1 if current_joke_id == total_jokes else current_joke_id + 1
    write_counter(next_id)

    return jsonify({"joke_id": selected_joke['id'], "joke": selected_joke['joke']})

def create_app():
    initialize_database()
    return app
