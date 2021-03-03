DROP procedure IF EXISTS dwh_challenge.`Load_Mail_Campaign_performance`;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE dwh_challenge.`Load_Mail_Campaign_performance`()
BEGIN

/*Dropping existing indexes*/
DROP INDEX ix_event_data_userid on event_data;
DROP INDEX ux_user_data_userid on user_data;

/*Creating index on user_id*/    
CREATE UNIQUE INDEX ux_user_data_userid on user_data
(
	user_id
);

CREATE INDEX ix_event_data_userid on event_data
(
	user_id,
    week_number
);



TRUNCATE TABLE campaign_performance;


/*Inserting data into Tatrget table*/
    
EXPLAIN INSERT INTO campaign_performance
(
	 Week_number
	,anonymized_user_id
	,provider_domain
	,provider_event_rate
	,overall_event_rate
)
WITH 
T as
(select week_number, u.email, u.user_id as user_id,
/*Capturing count of events triggered by individual user with a particular provider for a given week*/
count(*) over(partition by week_number, u.email,u.user_id) as user_event_count, 
/*Capturing count of events triggered by all users with above user's provider for a given week*/
count(*) over(partition by week_number, u.email) as provider_event_count, 
/*Capturing count of all events triggered by all users for a given week*/
count(*) over(partition by week_number) as Overall_event_count 
/*joining with user_data table on user_id*/
From 
 user_data u join event_data e 
 on e.user_id = u.user_id) 
select 
distinct T.week_number,
T.user_id as Anonymized_User_ID,
/*retrieving provider name from email*/
left(T.email,length(T.email)-5) as Provider_domain,
/*calculating provider event rate by dividing user_event_count and provider_event_count and rounding it of to 2 decimals*/
round(cast(T.user_event_count as Float)/cast(T.provider_event_count as Float),2) as Provider_event_rate,
/*calculating overall event rate by dividing user_event_count and overall_event_count and rounding it of to 2 decimals*/
round(cast(T.user_event_count as Float)/cast(T.Overall_event_count as Float),2) as Overall_event_rate 
from T; 


END$$
DELIMITER ;
