TRUNCATE salary_per_hour;

INSERT INTO salary_per_hour(
	salary_u,
    branch_id,
    year_month,
    salary_per_hour
)
with all_data as(
	select to_char(date("date"),'YYYY-MM') as year_month, *, 
	checkout - checkin as total from timesheets t
	left join employees e on t.employee_id = e.employe_id
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
left join total_hour a on d.branch_id = a.branch_id;
