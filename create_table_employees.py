import psycopg2
import json


conn =  psycopg2.connect(database='mekari_salary',
                    user='postgres', 
                    password='postgres', 
                    host='localhost'
)
cur=conn.cursor()

cur.execute("""CREATE TABLE employees(
    employee_id text PRIMARY KEY,
    branch_id text,
    salary integer,
    join_date date,
    resign_date date)
    """)

conn.commit()