DROP TABLE IF EXISTS `active_users`;

CREATE TABLE `active_users` (
  `id` int NOT NULL AUTO_INCREMENT,
  `Active_users` int DEFAULT NULL,
  `Consecutive_days_per_user` int DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



DROP TABLE IF EXISTS `campaign_performance`;

CREATE TABLE `campaign_performance` (
  `Week_number` int DEFAULT NULL,
  `anonymized_user_id` varchar(255) DEFAULT NULL,
  `provider_domain` varchar(255) DEFAULT NULL,
  `provider_event_rate` decimal(10,2) DEFAULT NULL,
  `overall_event_rate` decimal(10,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


DROP TABLE IF EXISTS `event_data`;

CREATE TABLE `event_data` (
  `id` int NOT NULL AUTO_INCREMENT,
  `event_date` date DEFAULT NULL,
  `event_id` int DEFAULT NULL,
  `user_id` int DEFAULT NULL,
  `week_number` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_event_data_userid` (`user_id`,`week_number`)
) ENGINE=InnoDB AUTO_INCREMENT=101 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



DROP TABLE IF EXISTS `user_data`;

CREATE TABLE `user_data` (
  `user_id` int NOT NULL,
  `email` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`user_id`),
  KEY `ux_user_data_userid` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;




