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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_schema_change") {
    def tableName = "test_schema_change"
    onFinish {
        // try_sql("DROP TABLE IF EXISTS ${tableName}")
    }

    def getAlterTableState = {
        waitForSchemaChangeDone {
            sql """ SHOW ALTER TABLE COLUMN WHERE tablename='${tableName}' ORDER BY createtime DESC LIMIT 1 """
            time 600
        }
        return true
    }

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `date` DATE NOT NULL COMMENT "数据灌入日期时间",
            `city` VARCHAR(20) COMMENT "用户所在城市",
            `age` SMALLINT COMMENT "用户年龄",
            `sex` TINYINT COMMENT "用户性别",
            `last_visit_date` DATETIME DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
            `last_update_date` DATETIME DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次更新时间",
            `last_visit_date_not_null` DATETIME NOT NULL DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
            `cost` BIGINT DEFAULT "0" COMMENT "用户总消费",
            `comment` VARCHAR(5),
            `max_dwell_time` INT DEFAULT "0" COMMENT "用户最大停留时间",
            `min_dwell_time` INT DEFAULT "99999" COMMENT "用户最小停留时间")
        UNIQUE KEY(`user_id`, `date`, `city`, `age`, `sex`)
        CLUSTER BY(`cost`, `comment`)
        DISTRIBUTED BY HASH(`user_id`)
        PROPERTIES ( "replication_num" = "1",
                     "enable_unique_key_merge_on_write" = "true"
        );
    """

    // 1. add a value column(any position after key column)
    for (int i = 0; i < 2; i++) {
        if (i == 1) {
            sql """ alter table ${tableName} ADD column score int after sex; """
            assertTrue(getAlterTableState(), "add column should success")
        }
        sql """ INSERT INTO ${tableName}
        (`user_id`, `date`, `city`, `age`, `sex`, `last_visit_date`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (1, '2017-10-01', 'Beijing', 10, 1, '2020-01-01', '2020-01-01', '2020-01-01', 1, 30, 20)
        """

        sql """ INSERT INTO ${tableName}
        (`user_id`, `date`, `city`, `age`, `sex`, `last_visit_date`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (2, '2017-10-01', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2020-01-02', 1, 31, 21)
        """

        sql """ INSERT INTO ${tableName}
        (`user_id`, `date`, `city`, `age`, `sex`, `last_visit_date`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (3, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20)
        """

        sql """ INSERT INTO ${tableName}
        (`user_id`, `date`, `city`, `age`, `sex`, `last_visit_date`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (4, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 22)
        """

        sql """ INSERT INTO ${tableName} 
        (`user_id`, `date`, `city`, `age`, `sex`, `last_visit_date`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`)
        VALUES (5, '2017-10-01', 'Beijing', 10, 1, NULL, NULL, '2020-01-05', 1, 34, 20)
        """

        qt_sql """ SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_visit_date`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t ORDER BY user_id; """

        // insert a duplicate key
        sql """ INSERT INTO ${tableName} 
        (`user_id`, `date`, `city`, `age`, `sex`, `last_visit_date`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`)
        VALUES (5, '2017-10-01', 'Beijing', 10, 1, NULL, NULL, '2020-01-05', 1, 34, 21)
        """
        qt_sql """ SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_visit_date`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t ORDER BY user_id; """

        // insert a duplicate key
        sql """ INSERT INTO ${tableName}
        (`user_id`, `date`, `city`, `age`, `sex`, `last_visit_date`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (5, '2017-10-01', 'Beijing', 10, 1, NULL, NULL, '2020-01-05', 1, 34, 22)
        """
        qt_sql """ SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_visit_date`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t ORDER BY user_id; """

        qt_sql """
        SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_visit_date`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t
        where user_id = 5;
        """

        qt_sql """ SELECT COUNT(*) FROM ${tableName};"""

        // insert a new key
        sql """ INSERT INTO ${tableName}
        (`user_id`, `date`, `city`, `age`, `sex`, `last_visit_date`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (6, '2017-10-01', 'Beijing', 10, 1, NULL, NULL, '2020-01-05', 1, 34, 22)
        """
        qt_sql """ SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_visit_date`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t ORDER BY user_id; """

        // insert batch key
        sql """ INSERT INTO ${tableName}
            (`user_id`, `date`, `city`, `age`, `sex`, `last_visit_date`, `last_update_date`, `last_visit_date_not_null`,
            `cost`, `max_dwell_time`, `min_dwell_time`) 
            VALUES
            (7, '2017-10-01', 'Beijing', 10, 1, NULL, NULL, '2020-01-05', 1, 34, 22),
            (7, '2017-10-01', 'Beijing', 10, 1, NULL, NULL, '2020-01-05', 1, 34, 23),
            (7, '2017-10-01', 'Beijing', 10, 1, NULL, NULL, '2020-01-05', 1, 34, 24),
            (7, '2017-10-01', 'Beijing', 10, 1, NULL, NULL, '2020-01-05', 1, 34, 25)
        """
        qt_sql """ SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_visit_date`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t ORDER BY user_id; """
        qt_sql """
        SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_visit_date`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t
        where user_id = 6 and sex = 1 ORDER BY user_id;
        """
    }

    // 2. drop a value column
    sql """ alter table ${tableName} DROP column last_visit_date; """
    assertTrue(getAlterTableState(), "drop column should success");
    {
        sql """ INSERT INTO ${tableName}
        (`user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (1, '2017-10-01', 'Beijing', 10, 1, '2020-01-01', '2020-01-01', 1, 30, 20)
        """

        sql """ INSERT INTO ${tableName}
        (`user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (2, '2017-10-01', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', 1, 31, 21)
        """

        sql """ INSERT INTO ${tableName}
        (`user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (3, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', 1, 32, 20)
        """

        sql """ INSERT INTO ${tableName}
        (`user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (4, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', 1, 32, 22)
        """

        sql """ INSERT INTO ${tableName} 
        (`user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (5, '2017-10-01', 'Beijing', 10, 1, NULL, '2020-01-05', 1, 34, 20)
        """

        qt_sql """ SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t ORDER BY user_id; """
        qt_sql """ SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t 
        where user_id = 6 and sex = 1 ORDER BY user_id; """

        // insert a duplicate key
        sql """ INSERT INTO ${tableName} 
        (`user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (5, '2017-10-01', 'Beijing', 10, 1, NULL, '2020-01-05', 1, 34, 21)
        """
        qt_sql """ SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t ORDER BY user_id; """

        // insert a duplicate key
        sql """ INSERT INTO ${tableName}
        (`user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (5, '2017-10-01', 'Beijing', 10, 1, NULL, '2020-01-05', 1, 34, 22)
        """
        qt_sql """ SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t ORDER BY user_id; """

        qt_sql """ SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t 
        where user_id = 5; """

        qt_sql """ SELECT COUNT(*) FROM ${tableName};"""

        // insert a new key
        sql """ INSERT INTO ${tableName}
        (`user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (6, '2017-10-01', 'Beijing', 10, 1, NULL, '2020-01-05', 1, 34, 22)
        """
        qt_sql """ SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t ORDER BY user_id; """

        // insert batch key
        sql """ INSERT INTO ${tableName}
            (`user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`,
            `cost`, `max_dwell_time`, `min_dwell_time`) 
            VALUES
            (7, '2017-10-01', 'Beijing', 10, 1, NULL, '2020-01-05', 1, 34, 22),
            (7, '2017-10-01', 'Beijing', 10, 1, NULL, '2020-01-05', 1, 34, 23),
            (7, '2017-10-01', 'Beijing', 10, 1, NULL, '2020-01-05', 1, 34, 24),
            (7, '2017-10-01', 'Beijing', 10, 1, NULL, '2020-01-05', 1, 34, 25)
        """
        qt_sql """ SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t ORDER BY user_id; """
        qt_sql """ SELECT * FROM ${tableName} t 
        where user_id = 6 and sex = 1 ORDER BY user_id; """
    }

    // 3.0 add a cluster key key column is not support
    // 3.1 drop a cluster key column is not support
    // unique table key column also can not be dropped: Can not drop key column in Unique data model table
    test {
        sql """ alter table ${tableName} DROP column cost; """
        exception "Can not drop cluster key column in Unique data model table"
    }

    // 4. modify a cluster key column
    test {
        sql """ alter table ${tableName} MODIFY column `comment` varchar(20); """
        exception "Can not modify cluster key column"
    }

    // 5. modify column order should success (Temporarily throw exception)
    test {
        sql """
            alter table ${tableName} ORDER BY (`user_id`, `date`, `city`, `age`, `sex`, `max_dwell_time`, `comment`, `min_dwell_time`, `last_visit_date_not_null`, `cost`, `score`, `last_update_date`);
        """
        exception "Can not modify column order in Unique data model table"
    }
    /*assertTrue(getAlterTableState(), "alter column order should success");
    {
        sql """ INSERT INTO ${tableName}
        (`user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (1, '2017-10-01', 'Beijing', 10, 1, '2020-01-01', '2020-01-01', 1, 30, 20)
        """

        sql """ INSERT INTO ${tableName}
        (`user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (2, '2017-10-01', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', 1, 31, 21)
        """

        sql """ INSERT INTO ${tableName}
        (`user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (3, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', 1, 32, 20)
        """

        sql """ INSERT INTO ${tableName}
        (`user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (4, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', 1, 32, 22)
        """

        sql """ INSERT INTO ${tableName} 
        (`user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (5, '2017-10-01', 'Beijing', 10, 1, NULL, '2020-01-05', 1, 34, 20)
        """

        qt_sql """ SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t ORDER BY user_id; """
        qt_sql """ SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t 
        where user_id = 6 and sex = 1 ORDER BY user_id; """

        // insert a duplicate key
        sql """ INSERT INTO ${tableName} 
        (`user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (5, '2017-10-01', 'Beijing', 10, 1, NULL, '2020-01-05', 1, 34, 21)
        """
        qt_sql """ SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t ORDER BY user_id; """

        // insert a duplicate key
        sql """ INSERT INTO ${tableName}
        (`user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (5, '2017-10-01', 'Beijing', 10, 1, NULL, '2020-01-05', 1, 34, 22)
        """
        qt_sql """ SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t ORDER BY user_id; """

        qt_sql """ SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t where user_id = 5; """

        qt_sql """ SELECT COUNT(*) FROM ${tableName};"""

        // insert a new key
        sql """ INSERT INTO ${tableName}
        (`user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`,
        `cost`, `max_dwell_time`, `min_dwell_time`) 
        VALUES (6, '2017-10-01', 'Beijing', 10, 1, NULL, '2020-01-05', 1, 34, 22)
        """
        qt_sql """ SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t ORDER BY user_id; """

        // insert batch key
        sql """ INSERT INTO ${tableName}
            (`user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`,
            `cost`, `max_dwell_time`, `min_dwell_time`) 
            VALUES
            (7, '2017-10-01', 'Beijing', 10, 1, NULL, '2020-01-05', 1, 34, 22),
            (7, '2017-10-01', 'Beijing', 10, 1, NULL, '2020-01-05', 1, 34, 23),
            (7, '2017-10-01', 'Beijing', 10, 1, NULL, '2020-01-05', 1, 34, 24),
            (7, '2017-10-01', 'Beijing', 10, 1, NULL, '2020-01-05', 1, 34, 25)
        """
        qt_sql """ SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t ORDER BY user_id; """
        qt_sql """ SELECT `user_id`, `date`, `city`, `age`, `sex`, `last_update_date`, `last_visit_date_not_null`, `cost`, `comment`, `max_dwell_time`, `min_dwell_time` FROM ${tableName} t 
        where user_id = 6 and sex = 1 ORDER BY user_id; """
    }*/
}
