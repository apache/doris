CREATE TABLE `agent` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `host` varchar(128) DEFAULT NULL COMMENT 'agent host',
  `port` int(8) DEFAULT NULL COMMENT 'agent port',
  `install_dir` varchar(256) DEFAULT NULL COMMENT 'agent install dir',
  `status` varchar(256) DEFAULT NULL COMMENT 'agent status',
  `register_time` timestamp  NULL  COMMENT 'register_time',
  `last_reported_time` timestamp NULL  COMMENT 'last report time',
  `cluster_id` int DEFAULT NULL COMMENT 'cluster id'
  PRIMARY KEY (`id`)
  UNIQUE KEY `agent_index` (`host`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE `agent_role` (
  `host` varchar(128) DEFAULT NULL COMMENT 'host',
  `cluster_id` int DEFAULT NULL COMMENT 'cluster id',
  `role` varchar(8) DEFAULT NULL COMMENT 'BE/FE',
  `fe_node_type` varchar(8) DEFAULT NULL COMMENT 'fe node type:FOLLOWER/OBSERVER',
  `install_dir` varchar(256) DEFAULT NULL COMMENT 'doris install dir',
  `status` varchar(256) DEFAULT NULL COMMENT 'status',
  UNIQUE KEY `host_index` (`host`,`role`,`cluster_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `process_instance` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'process id',
  `cluster_id` int DEFAULT NULL COMMENT 'cluster id',
  `user` varchar(135) DEFAULT NULL COMMENT 'execute user',
  `process_type` varchar(135) DEFAULT NULL COMMENT 'what step  execution to',
  `create_time` timestamp  NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`)
  UNIQUE KEY `task_index` (`user`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `task_instance` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'task id',
  `process_id` int(11) DEFAULT NULL COMMENT 'process id',
  `host` varchar(128) DEFAULT NULL COMMENT 'execute host',
  `task_type` int(11) DEFAULT NULL COMMENT 'install agent,install service,deploy conf,start/stop service etc.',
  `status` tinyint(4) DEFAULT NULL COMMENT 'task instance Status: 0 commit succeeded, 1 running, 2 fail, 3 succeed',
  `start_time` datetime DEFAULT NULL COMMENT 'task instance start time',
  `end_time` datetime DEFAULT NULL COMMENT 'task instance end time',
  `executor_id` varchar(512) NOT NULL COMMENT 'executor id,agent task id',
  `result` longtext DEFAULT NULL COMMENT 'execute result'
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;