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


suite("test_convert_tz") {
    sql "set enable_fallback_to_original_planner=false"
    sql "drop table if exists test_convert_tz;"
    sql """CREATE TABLE test_convert_tz
    (
            timestamp DATETIME NOT NULL
    )
    ENGINE = olap
    PARTITION BY range (timestamp)
    (
            PARTITION `p1` VALUES LESS THAN ('2021-01-01'),
    PARTITION `p2` VALUES LESS THAN ('2021-02-01'),
    PARTITION `p3` VALUES LESS THAN ('2021-03-01')
    ) DISTRIBUTED BY HASH (timestamp)
    PROPERTIES(
            "storage_format" = "DEFAULT",
            "replication_num" = "1");"""
    sql """INSERT INTO test_convert_tz (timestamp)
    VALUES ('2020-12-31'),
    ('2021-01-05'),
    ('2021-01-15'),
    ('2021-02-05'),
    ('2021-02-15');"""

    explain {
        sql "SELECT * FROM test_convert_tz WHERE convert_tz(timestamp, 'Asia/Shanghai', 'Europe/Paris') < '2021-01-01';"
        contains("partitions=2/3 (p1,p2)")
    }
    explain {
        sql "SELECT * FROM test_convert_tz WHERE convert_tz(timestamp, 'Asia/Shanghai', 'Europe/Paris') > '2021-01-01';";
        contains("partitions=2/3 (p2,p3)")
    }

    explain {
        sql """SELECT * FROM test_convert_tz WHERE convert_tz(timestamp, 'Asia/Shanghai', 'Europe/Paris') < '2021-02-24'
        and convert_tz(timestamp, 'Asia/Shanghai', 'Europe/Paris') > '2021-01-01';"""
        contains("partitions=2/3 (p2,p3)")
    }

    explain {
        sql """SELECT * FROM test_convert_tz WHERE convert_tz(timestamp, 'Asia/Shanghai', 'Europe/Paris') < '2021-02-24'
        or convert_tz(timestamp, 'Asia/Shanghai', 'Europe/Paris') > '2021-01-01';"""
        contains("partitions=3/3 (p1,p2,p3)")
    }

    explain {
        sql "SELECT * FROM test_convert_tz WHERE convert_tz(timestamp, 'Asia/Beijing', 'Europe/Paris') is null;";
        contains("partitions=3/3 (p1,p2,p3)")
    }

    explain {
        sql "SELECT * FROM test_convert_tz WHERE convert_tz(timestamp, 'Asia/Beijing', 'Europe/Paris') is not null;";
        contains("partitions=3/3 (p1,p2,p3)")
    }

    explain {
        sql "SELECT * FROM test_convert_tz WHERE date_trunc(convert_tz(timestamp, 'Asia/Beijing', 'Europe/Paris'), 'month') <'2021-01-01';";
        contains("partitions=3/3 (p1,p2,p3)")
    }

    explain {
        sql "SELECT * FROM test_convert_tz WHERE date_trunc(convert_tz(timestamp, 'Asia/Shanghai', 'Europe/Paris'), 'month') <'2021-01-01';";
        contains("partitions=2/3 (p1,p2)")
    }

    explain {
        sql "SELECT * FROM test_convert_tz WHERE convert_tz(date_trunc(timestamp, 'month'), 'Asia/Shanghai', 'Europe/Paris') <'2021-01-01';";
        contains("partitions=2/3 (p1,p2)")
    }
    for (int i = 0; i < 2; i++) {
        if (i == 0) {
            sql "set disable_nereids_rules = 'REWRITE_FILTER_EXPRESSION'"
        } else {
            sql "set disable_nereids_rules = ''"
        }
        explain {
            sql "SELECT * FROM test_convert_tz WHERE not convert_tz(timestamp, 'Asia/Shanghai', 'Europe/Paris') <= '2021-01-01';";
            contains("partitions=2/3 (p2,p3)")
        }
    }
}