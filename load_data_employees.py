import psycopg2
import json
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy

#the connection details
conn_string = 'postgresql://postgres:postgres@localhost/mekari_salary'

#read data from csv
df = pd.read_csv('task_mekari/employees.csv') 
df['resign_date'] = df['resign_date'].astype('datetime64[ns]')

print(df.head())

# to_sql - load data to database
db = create_engine(conn_string)
df.to_sql('employees', con=db, if_exists='replace', method="multi",
          index=False,
          dtype={'salary': sqlalchemy.types.INTEGER(), 
                   'employe_id': sqlalchemy.types.VARCHAR(),
                   'branch_id': sqlalchemy.types.VARCHAR(),
                   'join_date': sqlalchemy.types.Date(),
                   'resign_date': sqlalchemy.types.Date()})

#read data from database
#make new connection
connpsy = psycopg2.connect(conn_string) 
cursor = connpsy.cursor() 
#query read data
sql1 = '''select * from employees;'''
cursor.execute(sql1) 
for i in cursor.fetchall(): 
    print(i) 

connpsy.close() 

# for index, row in df.iterrows():
#     print(index)
#     print(row)
#     # Define the INSERT SQL command
#     insert_query = "INSERT INTO employees VALUES (%s, %s, %s, %s, %s);"
#     # Get the values from the current row as a list
#     values = list(row)
#     # Execute the SQL command with the values
#     cur.execute(insert_query, values)

# cur.execute("""select * from employees;
#     """)

# result=cur.fetchall()

# print(result)