create database case_study;
use case_study;

-- Importing the datasets
select * from demo;
select * from calls_north_centre;
select * from calls_southeast_centre;
select * from open_accounts;
select * from transactions;

-- updating datatype for date in each of the files
update demo
set dob=if(dob='',NULL,dob);
alter table demo
modify column dob date;
-- updating demo for uniform marital_status
update demo
set marital_status = 'married'
where marital_status = 'marrried';
select * from demo;

update calls_north_centre
set called_on=if(called_on='',NULL,called_on);
alter table calls_north_centre
modify column called_on date;

update open_accounts
set open_date=if(open_date='',NULL,open_date);
alter table open_accounts
modify column open_date date;

update transactions
set tran_dt=if(tran_dt='',NULL,tran_dt);
alter table transactions
modify column tran_dt date;

update calls_southeast_centre
set called_on=if(called_on='',NULL,called_on);
alter table calls_southeast_centre
modify column called_on date;

-- converting the duration of calls in secs in south-east region
update calls_southeast_centre
set durationmins = round(durationmins * 60,2);

-- creating view for the combined dataset for calls from north, south and east region
create view north_south_east as (select a.call_id, a.account_no, a.call_type, a.durationsecs, a.agent_name, a.called_on, b.cust_id, "North" as call_region
from calls_north_centre as a
left join open_accounts as b
on a.account_no = b.account_no
union all
select a.call_id, a.account_no, a.call_type, a.durationmins, a.agent_name, a.called_on, b.cust_id, "South-East" as call_region
from calls_southeast_centre as a
left join open_accounts as b
on a.account_no = b.account_no);

-- creating final output table
create table final_table (
cust_id bigint primary key,
no_of_open_accounts int,
no_of_inbound_calls int,
total_balance double,
gender varchar(10),
marital_status varchar(10),
avg_time_on_calls double, 
no_of_days_since_last_call int,
month_on_books int,
age_of_cust int,
no_of_debit_trans int,
no_of_credit_trans int,
open_balance_saving double,
open_balance_current double, 
no_of_trans int, 
amt_of_debit_trans double,
amt_of_credit_trans double,
no_of_trans_in_march_2013 int,
no_of_trans_in_may_2013 int,
avg_no_of_trans double,
no_of_calls_from_other_region int, 
no_of_agent_talked_to int
);

select * from final_table;

insert into final_table(cust_id)
(select distinct cust_id from demo);

-- a. no. of open accounts
update final_table
set no_of_open_accounts = (select count(account_no)
from open_accounts
where final_table.cust_id = open_accounts.cust_id);

-- b. no. of inbound calls
update final_table
set no_of_inbound_calls = (
select count(call_type)
from north_south_east
where final_table.cust_id = north_south_east.cust_id and call_type = 'in');

-- c. total balance
update final_table
set total_balance = (
select sum(balances)
from
(select account_no, cust_id, account_type, balance, balance_type, open_date, case when balance_type = '+' then balance
when balance_type = '-' then -1*balance end as balances 
from open_accounts) as d
where final_table.cust_id = d.cust_id);

-- d. gender
update final_table
set gender = (
select case when left(gender,1) = 'm' then "Male"
when left(gender, 1) = 'f' then "Female" end
from demo
where final_table.cust_id = demo.cust_id);

-- e. marital status
update final_table
set marital_status = (
select marital_status
from demo
where final_table.cust_id = demo.cust_id);

-- f.  Average time spent by a customer on all calls with the bank
update final_table
set avg_time_on_calls = (
select round(avg(durationsecs),2)
from north_south_east
where final_table.cust_id = north_south_east.cust_id);

-- g. Number of days since last call was received/made from/to the customer
update final_table
set no_of_days_since_last_call = (
select datediff(curdate(), max(called_on))
from north_south_east
where final_table.cust_id = north_south_east.cust_id);

--  h. Month on books
update final_table
set month_on_books = (
select timestampdiff(month, min(open_date), curdate())
from open_accounts
where final_table.cust_id = open_accounts.cust_id);

-- i. Age of the customer
update final_table
set age_of_cust = (
select timestampdiff(year, dob, curdate())
from demo where final_table.cust_id = demo.cust_id);
select * from final_table;

-- creating a view for transactions and mapping them with customer ids
create view trans_with_cust_id as (
select a.account_no, a.tran_id, a.tran_type, a.tran_amount, a.tran_dt, b.cust_id
from transactions as a
left join open_accounts as b
on a.account_no = b.account_no);

-- j.  Number of debit transactions
update final_table f
set no_of_debit_trans = (
select count(tran_id) 
from trans_with_cust_id t
where f.cust_id = t.cust_id and tran_type = 'debit');

-- k. Number of credit transactions
update final_table f
set no_of_credit_trans = (
select count(tran_id) 
from trans_with_cust_id t
where f.cust_id=t.cust_id and tran_type = 'credit');

-- l. open savings account
update final_table f
set open_balance_saving = (
select coalesce(sum(concat(balance_type, balance)), '-9999999')
from open_accounts o
where f.cust_id = o.cust_id and account_type in ('saving','savings'));

-- m open current account
update final_table f
set open_balance_current = (
select coalesce(sum(concat(balance_type, balance)), '-9999999')
from open_accounts o
where f.cust_id = o.cust_id and account_type = 'current');

-- n. Number of transactions
update final_table f
set no_of_trans = (
select count(tran_id)
from trans_with_cust_id t
where f.cust_id = t.cust_id);

-- o. amount of debit transactions
update final_table f
set amt_of_debit_trans = (
select coalesce(sum(tran_amount),0)
from trans_with_cust_id t
where f.cust_id = t.cust_id and tran_type = 'debit');

-- p. amount of credit transactions
update final_table f
set amt_of_credit_trans = (
select coalesce(sum(tran_amount),0)
from trans_with_cust_id t
where f.cust_id = t.cust_id and tran_type = 'credit');

-- q. no. of transactions in march 2013
update final_table f
set no_of_trans_in_march_2013 = (
select count(tran_id)
from trans_with_cust_id t
where f.cust_id = t.cust_id and tran_dt like '2013-03%');

-- r. no. of transactions in may 2013
update final_table f
set no_of_trans_in_may_2013 = (
select count(tran_id)
from trans_with_cust_id t
where f.cust_id = t.cust_id and tran_dt like '2013-05%');
select * from final_table;

-- s. average number of transactions
update final_table f
set avg_no_of_trans = (
select round(count(tran_id)/3,2)
from trans_with_cust_id t
where f.cust_id = t.cust_id);

-- t. no. of calls from different region
update final_table f
set no_of_calls_from_other_region = (
select sum(if(call_region = 'South-East' and left(account_no,3) = '124',1,0))+sum(if(call_region = 'North' and left(account_no,3) in ('171','117'),1,0))  
from north_south_east n
where f.cust_id = n.cust_id);

-- u. Number of agents a customer has talked 
update final_table f
set no_of_agent_talked_to = (
select count(distinct agent_name)
from north_south_east n
where f.cust_id = n.cust_id);
select * from final_table;

select * from final_table;

-- 2.  bi-variate frequency distribution of customers by gender & age-group
select case when age_of_cust < 24 then 'very young'
when age_of_cust between 25 and 35 then 'young'
when age_of_cust between 36 and 50 then 'middle-age'
when age_of_cust>=51 then 'old' end as age_group, 
sum(case when gender = 'Male' then 1 else 0 end) as male,
sum(case when gender = 'Female' then 1 else 0 end) as female
from final_table
group by 1
order by 1;
-- interpretation -> there are more males and females of middle - age group than any other age group

-- 3. Give a bi-variate frequency distribution of calls by “branch-region of account & region 
-- of call center”
select call_region, sum(case when branch_region = 'North' then 1 else 0 end) as north_region,
sum(case when branch_region = 'South' then 1 else 0 end) as south_region,
sum(case when branch_region = 'East' then 1 else 0 end) as east_region
from
(select *, case when left(account_no,3)='124' then 'North'
when left(account_no,3)='171' then 'East'
when left(account_no, 3)='117' then 'South' end as branch_region from north_south_east) as tab
group by 1
order by 1;
-- interpretation -> 
-- a. More calls were made from north region to the branches in the same region
-- b. less calls are made to the branches in different region than the call region

--  4. Is there any relationship between gender and total balance?
select gender, count(*) as total_count,
avg(total_balance) as avg_balance,
max(total_balance) as max_balance,
min(total_balance) as min_balance
from final_table
group by 1;
-- interpretation ->
-- a. females have more maximum balance, minimum balance and average balance than the males
-- b. there are more males than the females

--  5. Is there any relationship between age and total balance?
select round(sum((x-avg_x)*(y-avg_y))/(count(*)*stddev(x)*stddev(y)),4) as correlation 
from (select coalesce(age_of_cust,0) as x, total_balance as y,
(select avg(coalesce(age_of_cust,0)) from final_table) as avg_x, (select avg(total_balance) from final_table) as avg_y
from final_table) cc;
-- interpretation ->
-- with a unit change in the age of the customer, the total balance of the customer is expected to decrease by 0.0584 units

--  6. Is there any relationship between marital status and total balance
select marital_status, count(*) as total_count,
round(avg(total_balance),2) as avg_balance,
max(total_balance) as max_balance,
min(total_balance) as min_balance
from final_table
group by 1;
-- interpretation ->
-- a. there are less number of divorced people
-- b. no. of married and single people are almost same
-- c. the average, maximum and minimum balance of the married people is more than divorced and singles

-- 7.  Is there any relationship between gender and total number of transactions
select gender, count(no_of_trans) as total_count,
round(avg(no_of_trans),2) as avg_trans,
max(no_of_trans) as max_trans,
min(no_of_trans) as min_trans
from final_table
group by 1;
-- interpretation ->
-- a. there are more no. of transactions made by males than females
-- b. maximum transactions were made by a male

-- 8. Is there any relationship between number of calls and gender
SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));
with relation as (select a.cust_id, b.gender, count(call_id) as freq
from north_south_east a
left join final_table b
on a.cust_id = b.cust_id
group by 1)
select gender, count(freq) as no_of_calls, max(freq) as max_no_call, min(freq) as min_no_call, avg(freq) as avg_no_call
from relation
group by 1
order by 1;
-- interpretation ->
-- a. maximum no. of calls were made to a male
-- b. on an average, the duration of the call was longer for male than female

-- 9. Is there any relationship between number of debit transactions and gender
select gender, count(*) as total_count,
round(avg(no_of_debit_trans),2) as avg_deb_trans,
max(no_of_debit_trans) as max_deb_trans,
min(no_of_debit_trans) as min_deb_trans
from final_table
group by 1;
-- interpretation ->
-- a. maximum no. of debit transactions were made from a male
-- b. on an average the no. of debit transactions are almost same for male and female