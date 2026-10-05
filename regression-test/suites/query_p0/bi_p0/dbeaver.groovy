// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("dbeaver") {
    sql """SELECT  @@session.auto_increment_increment AS auto_increment_increment, @@character_set_client AS character_set_client, @@character_set_connection AS character_set_connection, @@character_set_results AS character_set_results, @@character_set_server AS character_set_server, @@collation_server AS collation_server, @@collation_connection AS collation_connection, @@init_connect AS init_connect, @@interactive_timeout AS interactive_timeout, @@license AS license, @@lower_case_table_names AS lower_case_table_names, @@max_allowed_packet AS max_allowed_packet, @@net_write_timeout AS net_write_timeout, @@performance_schema AS performance_schema, @@query_cache_size AS query_cache_size, @@query_cache_type AS query_cache_type, @@sql_mode AS sql_mode, @@system_time_zone AS system_time_zone, @@time_zone AS time_zone, @@transaction_isolation AS transaction_isolation, @@wait_timeout AS wait_timeout"""
    sql """SET character_set_results = NULL"""
    sql """SET autocommit=1"""
    sql """SET sql_mode='STRICT_TRANS_TABLES'"""
    sql """SET autocommit=1"""
    sql """SELECT DATABASE()"""
    sql """SHOW ENGINES"""
    sql """SHOW CHARSET"""
    sql """SHOW COLLATION"""
    sql """SELECT @@GLOBAL.character_set_server,@@GLOBAL.collation_server"""
    sql """SHOW PLUGINS"""
    sql """SHOW VARIABLES LIKE 'lower_case_table_names'"""
    sql """show databases"""
    sql """SELECT * FROM information_schema.TABLES t WHERE 	t.TABLE_SCHEMA = 'information_schema' 	AND t.TABLE_NAME = 'CHECK_CONSTRAINTS'"""
    sql """SHOW FULL PROCESSLIST"""
    sql """SHOW PRIVILEGES"""
    sql """SHOW GRANTS FOR 'admin'@'%'"""
    sql """SHOW STATUS"""
    sql """SHOW GLOBAL STATUS"""
    sql """SHOW VARIABLES"""
    sql """SHOW GLOBAL VARIABLES"""
    sql """SHOW FULL TABLES FROM `__internal_schema`"""
    sql """SHOW TABLE STATUS FROM `__internal_schema`"""
    sql """SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='__internal_schema' AND TABLE_NAME='column_statistics' ORDER BY ORDINAL_POSITION"""
    sql """SELECT * FROM mysql.user ORDER BY user"""
}