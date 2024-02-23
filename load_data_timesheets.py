import psycopg2
import json
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy

#the connection details
conn_string = 'postgresql://postgres:postgres@localhost/mekari_salary'

#read data from csv
df = pd.read_csv('task_mekari/timesheets.csv') 

print(df.head())

# to_sql - load data to database
db = create_engine(conn_string)
df.to_sql('timesheets', con=db, if_exists='replace', method="multi",
          index=False,
          dtype={'checkin': sqlalchemy.types.Time(),
                    'checkout': sqlalchemy.types.Time(), 
                   'timesheet_id': sqlalchemy.types.VARCHAR(),
                   'employee_id': sqlalchemy.types.VARCHAR(),
                   'date': sqlalchemy.types.Date()})

#read data from database
#make new connection
connpsy = psycopg2.connect(conn_string) 
cursor = connpsy.cursor() 
#query read data
sql1 = '''select * from timesheets;'''
cursor.execute(sql1) 
for i in cursor.fetchall(): 
    print(i) 

connpsy.close() 