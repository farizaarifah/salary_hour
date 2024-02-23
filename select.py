import psycopg2
import json
import pandas as pd

conn =  psycopg2.connect(database='mekari_salary',
                    user='postgres', 
                    password='postgres', 
                    host='localhost'
)
cur=conn.cursor()

cur.execute("""select * from salary_per_hour;
    """)

result=cur.fetchall()

column_names = ['salary_u','branch_id','year_month','salary_per_hour' ]
df = pd.DataFrame(result, columns=column_names)

print(df.head)