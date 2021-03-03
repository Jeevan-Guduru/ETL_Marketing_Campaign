DROP procedure IF EXISTS dwh_challenge.`Load_Active_Users`;

DELIMITER $$
CREATE  PROCEDURE dwh_challenge.`Load_Active_Users`()
BEGIN

TRUNCATE TABLE active_users;

Insert into dwh_challenge.Active_users
(
	Active_users,
    Consecutive_days_per_user
)
select 
user_id as Active_users,
/*counting the dates grouping by user_id and below calculated consecutive_dates */
count(*) as Consecutive_days_per_user 
from 
/* Substracting interval of below assigned  row number of days from the event date.
For a particular user, this will create groups of same dates.*/
(select *,(date-interval day_row_number day) as consecutive_dates 
from 
/*Calculating row number for dates partitioned by user_id, 
this will allocate numbers in serial to all the dates on which a user triggered the event */
(select *,row_number() over(PARTITION by user_id order by  date) as day_row_number
 from
 /*Fetching distinct dates for an event recorded by user , since an event can be triggered multiple times on the same day*/
(select 
DISTINCT date(event_date) as  date,user_id 
from dwh_challenge.event_data)a
)b
)c 
GROUP BY Active_users,consecutive_dates 
having count(*)>=3 
order by Active_users;


END$$
DELIMITER ;
