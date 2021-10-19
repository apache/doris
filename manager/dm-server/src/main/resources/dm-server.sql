CREATE TABLE `t_agent` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `host` varchar(128) DEFAULT NULL COMMENT 'agent host',
  `port` int(8) DEFAULT NULL COMMENT 'agent port',
  `install_dir` varchar(256) DEFAULT NULL COMMENT 'agent install dir',
  `status` varchar(256) DEFAULT NULL COMMENT 'agent status',
  `register_time` timestamp  NULL  COMMENT 'register_time',
  `last_reported_time` timestamp NULL  COMMENT 'last report time'
  PRIMARY KEY (`id`)
  UNIQUE KEY `agent_index` (`host`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE `t_agent_role` (
  `host` varchar(128) DEFAULT NULL COMMENT 'host',
  `role` varchar(8) DEFAULT NULL COMMENT 'BE/FE',
  `install_dir` varchar(256) DEFAULT NULL COMMENT 'doris install dir',
  UNIQUE KEY `host_index` (`host`,`role`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;