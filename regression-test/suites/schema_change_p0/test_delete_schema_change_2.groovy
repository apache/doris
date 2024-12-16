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

suite('test_delete_schema_chang_2') {
    sql 'DROP TABLE IF EXISTS schema_change_delete_regression_test_2;'

    sql '''
    CREATE TABLE IF NOT EXISTS schema_change_delete_regression_test_2 (
                    `user_id` LARGEINT NOT NULL COMMENT "用户id",
                    `date` DATE NOT NULL COMMENT "数据灌入日期时间",
                    `city` VARCHAR(20) COMMENT "用户所在城市",
                    `age` SMALLINT COMMENT "用户年龄",
                    `sex` TINYINT COMMENT "用户性别",
                    `last_visit_date` DATETIME DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
                    `last_update_date` DATETIME DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次更新时间",
                    `last_visit_date_not_null` DATETIME NOT NULL DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
                    `cost` BIGINT DEFAULT "0" COMMENT "用户总消费",
                    `max_dwell_time` INT DEFAULT "0" COMMENT "用户最大停留时间",
                    `min_dwell_time` INT DEFAULT "99999" COMMENT "用户最小停留时间")
                DUPLICATE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
                BUCKETS 8
                PROPERTIES ( "replication_num" = "1", "light_schema_change" = "false" );
    '''

    sql """
    INSERT INTO schema_change_delete_regression_test_2 VALUES
                 (1, '2017-10-01', 'Beijing', 10, 1, '2020-01-01', '2020-01-01', '2020-01-01', 1, 30, 20);
    """

    sql """
    INSERT INTO schema_change_delete_regression_test_2 VALUES
                 (2, '2017-10-01', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2020-01-02', 1, 31, 21);
    """

    sql 'SELECT * FROM schema_change_delete_regression_test_2 order by user_id ASC, last_visit_date;'

    if (!isCloudMode()) {
        sql "ALTER TABLE schema_change_delete_regression_test_2 SET ('light_schema_change' = 'true');"
    }

    sql "ALTER table schema_change_delete_regression_test_2 ADD COLUMN new_column INT default '1';"

    sql 'DELETE FROM schema_change_delete_regression_test_2 where new_column = 1;'
    
    sql 'SELECT * FROM schema_change_delete_regression_test_2 order by user_id DESC, last_visit_date;'

    sql """
    INSERT INTO schema_change_delete_regression_test_2 VALUES
                 (1, '2017-10-01', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2020-01-02', 1, 31, 19, 2);
    """

    sql """
    INSERT INTO schema_change_delete_regression_test_2 VALUES
                 (2, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20, 2);
    """

    sql """
    INSERT INTO schema_change_delete_regression_test_2 VALUES
                 (3, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20, 1);
    """

    sql """
    INSERT INTO schema_change_delete_regression_test_2 VALUES
                 (3, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20, 2);
    """

    sql 'SELECT * FROM schema_change_delete_regression_test_2 order by user_id DESC, last_visit_date;'
}
