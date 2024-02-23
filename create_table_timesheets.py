import psycopg2
import json


conn =  psycopg2.connect(database='mekari_salary',
                    user='postgres', 
                    password='postgres', 
                    host='localhost'
)
cur=conn.cursor()

cur.execute("""CREATE TABLE timesheets(
    timesheet_id text PRIMARY KEY,
    employee_id text,
    "date" date,
    checkin time,
    checkout time)
    """)

conn.commit()