import sqlite3
import pandas as pd
con=sqlite3.connect('./jokes.db')
cursor=con.cursor()

res=cursor.execute("SELECT joke from jokes;")

try:
    fetched_jokes_arr=res.fetchall()
finally:
    print(len(fetched_jokes_arr))
    df=pd.DataFrame(fetched_jokes_arr)
    df.to_csv('cleaned_jokes.csv')
