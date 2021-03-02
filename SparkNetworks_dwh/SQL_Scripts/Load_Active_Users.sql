DROP procedure IF EXISTS `Load_Active_Users`;

CREATE  PROCEDURE `Load_Active_Users`()
BEGIN

TRUNCATE TABLE active_users;

Insert into dwh_challenge.Active_users
(
	Active_users,
    Consecutive_days_per_user
)
select 
user_id as Active_users,
count(*) as Consecutive_days_per_user 
from 
(select *,(date-interval day_row_number day) as consecutive_dates 
from 
(select *,row_number() over(PARTITION by user_id order by  date) as day_row_number
 from
(select 
DISTINCT date(event_date) as  date,user_id 
from dwh_challenge.event_data)a
)b
)c 
GROUP BY Active_users,consecutive_dates 
having count(*)>=3 
order by Active_users;


END
