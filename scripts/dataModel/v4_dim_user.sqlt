select * from table(data_profile('sn_stg_users')) 
where n_null < (select count(*) from sn_stg_users) order by n_null asc;

select sys_id as row_wid,
active,
vip,
u_exec,
sys_updated_on,
sys_updated_by,
sys_created_on,
sys_created_by,
name,
first_name,
last_name,
source,
email,
manager,
user_name,
department,
city,
employee_number,
street,
title,
cost_center,
last_login_time,
location,
phone,
u_reporting_center
from sn_stg_users;