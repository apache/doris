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

suite("test_primary_key_simple_case") {
    def tableName = "primary_key_simple_case"
    onFinish {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
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
            `max_dwell_time` INT DEFAULT "0" COMMENT "用户最大停留时间",
            `min_dwell_time` INT DEFAULT "99999" COMMENT "用户最小停留时间")
        UNIQUE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
        PROPERTIES ( "replication_num" = "1",
                        "enable_unique_key_merge_on_write" = "true");
    """

    sql """ INSERT INTO ${tableName} VALUES
            (1, '2017-10-01', 'Beijing', 10, 1, '2020-01-01', '2020-01-01', '2020-01-01', 1, 30, 20)
        """

    sql """ INSERT INTO ${tableName} VALUES
            (2, '2017-10-01', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2020-01-02', 1, 31, 21)
        """

    sql """ INSERT INTO ${tableName} VALUES
            (3, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20)
        """

    sql """ INSERT INTO ${tableName} VALUES
            (4, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 22)
        """

    sql """ INSERT INTO ${tableName} VALUES
            (5, '2017-10-01', 'Beijing', 10, 1, NULL, NULL, '2020-01-05', 1, 34, 20)
        """

    def result = sql """ SELECT * FROM ${tableName} t ORDER BY user_id; """
    assertTrue(result.size() == 5)
    assertTrue(result[0].size() == 11)

    // insert a duplicate key
    sql """ INSERT INTO ${tableName} VALUES
            (5, '2017-10-01', 'Beijing', 10, 1, NULL, NULL, '2020-01-05', 1, 34, 21)
        """
    result = sql """ SELECT * FROM ${tableName} t ORDER BY user_id; """
    assertTrue(result.size() == 5)
    assertTrue(result[4][10] == 21)

    // insert a duplicate key
    sql """ INSERT INTO ${tableName} VALUES
            (5, '2017-10-01', 'Beijing', 10, 1, NULL, NULL, '2020-01-05', 1, 34, 22)
        """
    result = sql """ SELECT * FROM ${tableName} t ORDER BY user_id; """
    assertTrue(result.size() == 5)
    logger.info("fuck: " + result.size())
    assertTrue(result[4][10] == 22)

    result = sql """ SELECT * FROM ${tableName} t where user_id = 5; """
    assertTrue(result.size() == 1)
    assertTrue(result[0][10] == 22)

    result = sql """ SELECT COUNT(*) FROM ${tableName};"""
    assertTrue(result.size() == 1)
    assertTrue(result[0][0] == 5)

    // insert a new key
    sql """ INSERT INTO ${tableName} VALUES
            (6, '2017-10-01', 'Beijing', 10, 1, NULL, NULL, '2020-01-05', 1, 34, 22)
    """
    result = sql """ SELECT * FROM ${tableName} t ORDER BY user_id; """
    assertTrue(result.size() == 6)

    // insert batch key 
    sql """ INSERT INTO ${tableName} VALUES
            (7, '2017-10-01', 'Beijing', 10, 1, NULL, NULL, '2020-01-05', 1, 34, 22),
            (7, '2017-10-01', 'Beijing', 10, 1, NULL, NULL, '2020-01-05', 1, 34, 23),
            (7, '2017-10-01', 'Beijing', 10, 1, NULL, NULL, '2020-01-05', 1, 34, 24),
            (7, '2017-10-01', 'Beijing', 10, 1, NULL, NULL, '2020-01-05', 1, 34, 25)
    """
    result = sql """ SELECT * FROM ${tableName} t ORDER BY user_id; """
    assertTrue(result.size() == 7)
    assertTrue(result[6][10] == 25)

    sql """ DROP TABLE IF EXISTS test_unique_key_range_tbl """
    sql """
        create table test_unique_key_range_tbl (
            k1 date not null,
            k2 bigint not null,
            v1 int null,
            v2 int not null
        ) UNIQUE KEY(`k1`, `k2`)
        DISTRIBUTED BY HASH(`k2`) BUCKETS 30
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        insert into test_unique_key_range_tbl values
            ( '2024-02-18' , -7822995176885966013 ,    -10 ,     -4 ),
            ( '2024-02-18' , -5987215688096912139 ,      8 ,      2 ),
            ( '2024-02-18' , -5889932400568797810 ,    -10 ,    -10 ),
            ( '2024-02-18' , -5051784705055344649 ,      1 ,      6 ),
            ( '2024-02-18' , -4635608137995832373 ,      3 ,      9 ),
            ( '2024-02-18' , -3836821172182966892 ,    -10 ,    -10 ),
            ( '2024-02-18' , -3675645188438967877 ,   NULL ,     -4 ),
            ( '2024-02-18' , -3363157164254363034 ,      5 ,      4 ),
            ( '2024-02-18' ,  -849169574767655353 ,     -4 ,    -10 ),
            ( '2024-02-18' ,  -293023807696575395 ,   NULL ,      9 ),
            ( '2024-02-18' ,  1167104788249072527 ,      0 ,      4 ),
            ( '2024-02-18' ,  1660707941299238025 ,      9 ,      9 ),
            ( '2024-02-18' ,  2852819493813807984 ,      0 ,      6 ),
            ( '2024-02-18' ,  5444305694667795860 ,      9 ,      1 ),
            ( '2024-02-18' ,  6136152292926889790 ,      2 ,      9 ),
            ( '2024-02-18' ,  6538123407677174537 ,     -4 ,    -10 ),
            ( '2024-02-18' ,  7958269158967938474 ,    -10 ,      9 ),
            ( '2024-02-18' ,  9019386549208004184 ,    -10 ,      5 ),
            ( '2024-02-18' ,  9208781524087970597 ,      0 ,      5 );
    """

    qt_pk_key_range """
        select k1, v1, v2 from test_unique_key_range_tbl where k1 = '2024-02-18' order by 1, 2, 3;
    """
}
