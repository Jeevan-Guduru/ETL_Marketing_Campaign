CREATE DATABASE IF NOT EXISTS dwh_challenge;


DROP TABLE IF EXISTS dwh_challenge.`active_users`;

CREATE TABLE dwh_challenge.`active_users` (
  `id` int NOT NULL AUTO_INCREMENT,
  `Active_users` int DEFAULT NULL,
  `Consecutive_days_per_user` int DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;




DROP TABLE IF EXISTS dwh_challenge.`campaign_performance`;

CREATE TABLE dwh_challenge.`campaign_performance` (
  `Week_number` int DEFAULT NULL,
  `anonymized_user_id` varchar(255) DEFAULT NULL,
  `provider_domain` varchar(255) DEFAULT NULL,
  `provider_event_rate` decimal(10,2) DEFAULT NULL,
  `overall_event_rate` decimal(10,2) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



DROP TABLE IF EXISTS dwh_challenge.`event_data`;

CREATE TABLE `event_data` (
  `id` int NOT NULL AUTO_INCREMENT,
  `event_date` date DEFAULT NULL,
  `event_id` int DEFAULT NULL,
  `user_id` int DEFAULT NULL,
  `week_number` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_event_data_userid` (`user_id`,`week_number`)
) ENGINE=MyISAM AUTO_INCREMENT=1048575 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



DROP TABLE IF EXISTS dwh_challenge.`user_data`;

CREATE TABLE dwh_challenge.`user_data` (
  `user_id` int NOT NULL,
  `email` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`user_id`),
  UNIQUE KEY `ux_user_data_userid` (`user_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;





