import psycopg2
import json
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
from datetime import timedelta, datetime, date

now= datetime.now()
current_date_str = datetime(now.year, now.month, now.day-1, now.hour, 0, 0)

conn =  psycopg2.connect(database='mekari_salary',
                    user='postgres', 
                    password='postgres', 
                    host='localhost'
)
cur=conn.cursor()
cur.execute("""with all_data as(
	select to_char(date("date"),'YYYY-MM') as year_month, *, 
	checkout - checkin as total from timesheets t
	left join employees e on t.employee_id = e.employe_id
	where "date"={}
)
,distinct_employee_salary as(
	select sum(salary) as salary_u,
	year_month, branch_id
	from (select distinct employe_id, salary, year_month, branch_id from all_data)
	group by year_month, branch_id
)
,total_hour as (
	select branch_id, year_month, sum(total) as total_ from all_data
	group by branch_id, year_month)
select d.salary_u, d.branch_id, d.year_month,
salary_u/extract(hour from total_) as salary_per_hour
from distinct_employee_salary d
left join total_hour a on d.branch_id = a.branch_id
    """.format(current_date_str)
)
result=cur.fetchall()

column_names = ['salary_u','branch_id','year_month','salary_per_hour' ]
df = pd.DataFrame(result, columns=column_names)
print(df.head)

conn_string = 'postgresql://postgres:postgres@localhost/mekari_salary'
db = create_engine(conn_string)
df.to_sql('testing_result', con=db, if_exists='append', method="multi",
          index=False,
          dtype={'salary_u': sqlalchemy.types.INTEGER(), 
                   'branch_id': sqlalchemy.types.VARCHAR(),
                   'year_month': sqlalchemy.types.VARCHAR(),
                   'salary_per_hour': sqlalchemy.types.Float()}
        )


