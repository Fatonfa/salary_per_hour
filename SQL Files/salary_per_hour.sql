create schema if not exists analytic;

create table if not exists analytic.fact_salary_per_hour(
	year int4,
	month int4,
	branch_id int4,
	total_employees int4,
	total_salary int8,
	total_work_hour int4,
	salary_per_hour int8
);

truncate table analytic.fact_salary_per_hour;

insert into analytic.fact_salary_per_hour
with base as (
	select 
		employee_id
		, branch_id
		, salary
		, date_trunc('month', "date"::date)::date as month_date
		, date
		, coalesce(checkin, '00:00:00') as checkin
		, coalesce(checkout, '24:00:00') as checkout
		, coalesce(checkout, '24:00:00')::time - coalesce(checkin, '00:00:00')::time as hour_worked 
	from stg.timesheets t 
	left join stg.employees e on t.employee_id = e.employe_id 
), staging as (
	select
		branch_id
		, month_date
		, employee_id 
		, salary
		, sum(hour_worked) as hour_worked
	--	sum(hour_worked)
	from base 
	group by 1,2,3,4
	order by 1,2,3
)
select 
	extract(year from month_date) as year 
	, extract(month from month_date) as month
	, branch_id
	, count(distinct employee_id) as total_employees
	, sum(salary) as total_salary
	, sum(extract(hour from hour_worked)) as total_work_hour
	, (sum(salary) / sum(extract(hour from hour_worked)))::int4 as salary_per_hour
from staging
group by 1,2,3
order by 1,2,3
;