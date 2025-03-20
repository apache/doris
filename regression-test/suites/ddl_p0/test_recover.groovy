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

suite("test_recover") {
    try {
        for (int i = 0; i <= 7; i++) {
                sql """
            CREATE DATABASE IF NOT EXISTS `test_recover_db`
            """

                sql """
            CREATE TABLE IF NOT EXISTS `test_recover_db`.`test_recover_tb` (
            `k1` int(11) NULL,
            `k2` datetime NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`k1`)
            PARTITION BY RANGE(`k1`)
            (PARTITION p111 VALUES [('-1000'), ('111')),
            PARTITION p222 VALUES [('111'), ('222')),
            PARTITION p333 VALUES [('222'), ('333')),
            PARTITION p1000 VALUES [('333'), ('1000')))
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
            """

            // test drop/recover partition

                def res = sql """ SHOW CREATE TABLE `test_recover_db`.`test_recover_tb` """
                assertTrue(res.size() != 0)

                sql """
            ALTER TABLE `test_recover_db`.`test_recover_tb` DROP PARTITION p1000;
            """

                res = sql """ SHOW CREATE TABLE `test_recover_db`.`test_recover_tb` """
                assertTrue(res.size() != 0)

                sql """
            RECOVER PARTITION p1000 FROM `test_recover_db`.`test_recover_tb`
            """

                res = sql """ SHOW CREATE TABLE `test_recover_db`.`test_recover_tb` """
                assertTrue(res.size() != 0)

                sql """
            ALTER TABLE `test_recover_db`.`test_recover_tb` DROP PARTITION p1000
            """

                res = sql """ SHOW CREATE TABLE `test_recover_db`.`test_recover_tb` """
                assertTrue(res.size() != 0)

                sql """
            RECOVER PARTITION p1000 AS p2000 FROM `test_recover_db`.`test_recover_tb`
            """

                res = sql """ SHOW CREATE TABLE `test_recover_db`.`test_recover_tb` """
                assertTrue(res.size() != 0)

                sql """
            ALTER TABLE `test_recover_db`.`test_recover_tb` DROP PARTITION p2000
            """

                res = sql """ SHOW CREATE TABLE `test_recover_db`.`test_recover_tb` """
                assertTrue(res.size() != 0)

                sql """
            ALTER TABLE `test_recover_db`.`test_recover_tb` ADD PARTITION p2000 VALUES [('1000'), ('2000'))
            """

                res = sql """ SHOW CREATE TABLE `test_recover_db`.`test_recover_tb` """
                assertTrue(res.size() != 0)

                sql """
            ALTER TABLE `test_recover_db`.`test_recover_tb` DROP PARTITION p2000
            """
                sql """
            ALTER TABLE `test_recover_db`.`test_recover_tb` ADD PARTITION p2000 VALUES [('2000'), ('3000'))
            """

                res = sql """ SHOW CREATE TABLE `test_recover_db`.`test_recover_tb` """
                assertTrue(res.size() != 0)

                sql """
            ALTER TABLE `test_recover_db`.`test_recover_tb` DROP PARTITION p2000
            """
                sql """
            ALTER TABLE `test_recover_db`.`test_recover_tb` ADD PARTITION p2000 VALUES [('3000'), ('4000'))
            """

                res = sql """ SHOW CREATE TABLE `test_recover_db`.`test_recover_tb` """
                assertTrue(res.size() != 0)

                sql """
            ALTER TABLE `test_recover_db`.`test_recover_tb` DROP PARTITION p2000
            """
                sql """
            ALTER TABLE `test_recover_db`.`test_recover_tb` ADD PARTITION p2000 VALUES [('4000'), ('5000'))
            """

                res = sql """ SHOW CREATE TABLE `test_recover_db`.`test_recover_tb` """
                assertTrue(res.size() != 0)

                sql """
            ALTER TABLE `test_recover_db`.`test_recover_tb` DROP PARTITION p2000
            """

                res = sql """ SHOW CREATE TABLE `test_recover_db`.`test_recover_tb` """
                assertTrue(res.size() != 0)

                sql """
            RECOVER PARTITION p2000 FROM `test_recover_db`.`test_recover_tb`
            """

                res = sql """ SHOW CREATE TABLE `test_recover_db`.`test_recover_tb` """
                assertTrue(res.size() != 0)

                sql """
            RECOVER PARTITION p2000 AS p1000 FROM `test_recover_db`.`test_recover_tb`
            """

                res = sql """ SHOW CREATE TABLE `test_recover_db`.`test_recover_tb` """
                assertTrue(res.size() != 0)
                
            // test drop/recover table

                sql """
            DROP TABLE `test_recover_db`.`test_recover_tb`
            """

                qt_select """SHOW TABLES FROM `test_recover_db`"""

                sql """
            RECOVER TABLE `test_recover_db`.`test_recover_tb`
            """

                qt_select """SHOW TABLES FROM `test_recover_db`"""

                sql """
            DROP TABLE `test_recover_db`.`test_recover_tb`
            """

                qt_select """SHOW TABLES FROM `test_recover_db`"""

                sql """
            CREATE TABLE `test_recover_db`.`test_recover_tb` (
            `k1` int(11) NULL,
            `k2` datetime NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`k1`)
            PARTITION BY RANGE(`k1`)
            (PARTITION p111 VALUES [('-1000'), ('111')),
            PARTITION p222 VALUES [('111'), ('222')),
            PARTITION p333 VALUES [('222'), ('333')),
            PARTITION p1000 VALUES [('333'), ('1000')))
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
            """

                qt_select """SHOW TABLES FROM `test_recover_db`"""

                sql """
            DROP TABLE `test_recover_db`.`test_recover_tb`
            """
                sql """
            CREATE TABLE `test_recover_db`.`test_recover_tb` (
            `k1` int(11) NULL,
            `k2` datetime NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`k1`)
            PARTITION BY RANGE(`k1`)
            (PARTITION p111 VALUES [('-1000'), ('111')),
            PARTITION p222 VALUES [('111'), ('222')),
            PARTITION p333 VALUES [('222'), ('333')),
            PARTITION p1000 VALUES [('333'), ('1000')))
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
            """

                qt_select """SHOW TABLES FROM `test_recover_db`"""

                sql """
            DROP TABLE `test_recover_db`.`test_recover_tb`
            """
                sql """
            CREATE TABLE `test_recover_db`.`test_recover_tb` (
            `k1` int(11) NULL,
            `k2` datetime NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`k1`)
            PARTITION BY RANGE(`k1`)
            (PARTITION p111 VALUES [('-1000'), ('111')),
            PARTITION p222 VALUES [('111'), ('222')),
            PARTITION p333 VALUES [('222'), ('333')),
            PARTITION p1000 VALUES [('333'), ('1000')))
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
            """

                qt_select """SHOW TABLES FROM `test_recover_db`"""

                sql """
            DROP TABLE `test_recover_db`.`test_recover_tb`
            """
                sql """
            CREATE TABLE `test_recover_db`.`test_recover_tb` (
            `k1` int(11) NULL,
            `k2` datetime NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`k1`)
            PARTITION BY RANGE(`k1`)
            (PARTITION p111 VALUES [('-1000'), ('111')),
            PARTITION p222 VALUES [('111'), ('222')),
            PARTITION p333 VALUES [('222'), ('333')),
            PARTITION p1000 VALUES [('333'), ('1000')))
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
            """

                qt_select """SHOW TABLES FROM `test_recover_db`"""

                sql """
            DROP TABLE `test_recover_db`.`test_recover_tb`
            """

                qt_select """SHOW TABLES FROM `test_recover_db`"""

                sql """
            RECOVER TABLE `test_recover_db`.`test_recover_tb` AS `test_recover_tb_new`
            """

                qt_select """SHOW TABLES FROM `test_recover_db`"""

                sql """
            RECOVER TABLE `test_recover_db`.`test_recover_tb`
            """

                qt_select """SHOW TABLES FROM `test_recover_db`"""

                
            // test drop/recover db

                def showDatabase = sql """ SHOW CREATE DATABASE test_recover_db """
                assertTrue(showDatabase[0][1].contains("CREATE DATABASE `test_recover_db`"))

                sql """
            DROP DATABASE `test_recover_db`
            """

                sql """
            RECOVER DATABASE `test_recover_db`
            """

                showDatabase = sql """ SHOW CREATE DATABASE test_recover_db """
                assertTrue(showDatabase[0][1].contains("CREATE DATABASE `test_recover_db`"))

                sql """
            CREATE TABLE `test_recover_db`.`test_recover_tb_1` (
            `k1` int(11) NULL,
            `k2` datetime NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`k1`)
            PARTITION BY RANGE(`k1`)
            (PARTITION p111 VALUES [('-1000'), ('111')),
            PARTITION p222 VALUES [('111'), ('222')),
            PARTITION p333 VALUES [('222'), ('333')),
            PARTITION p1000 VALUES [('333'), ('1000')))
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
            """

                sql """
            DROP DATABASE `test_recover_db`
            """

                sql """
            CREATE DATABASE `test_recover_db`
            """

                showDatabase = sql """ SHOW CREATE DATABASE `test_recover_db` """
                assertTrue(showDatabase[0][1].contains("CREATE DATABASE `test_recover_db`"))

                sql """
            DROP DATABASE `test_recover_db`
            """
                sql """
            CREATE DATABASE `test_recover_db`
            """

                showDatabase = sql """ SHOW CREATE DATABASE `test_recover_db` """
                assertTrue(showDatabase[0][1].contains("CREATE DATABASE `test_recover_db`"))

                sql """
            DROP DATABASE `test_recover_db`
            """

                sql """
            CREATE DATABASE `test_recover_db`
            """

                showDatabase = sql """ SHOW CREATE DATABASE `test_recover_db` """
                assertTrue(showDatabase[0][1].contains("CREATE DATABASE `test_recover_db`"))
                
                sql """
            CREATE TABLE `test_recover_db`.`test_recover_tb` (
            `k1` int(11) NULL,
            `k2` datetime NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`k1`)
            PARTITION BY RANGE(`k1`)
            (PARTITION p111 VALUES [('-1000'), ('111')),
            PARTITION p222 VALUES [('111'), ('222')),
            PARTITION p333 VALUES [('222'), ('333')),
            PARTITION p1000 VALUES [('333'), ('1000')))
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
            """

                sql """
            DROP DATABASE `test_recover_db`
            """
                sql """
            CREATE DATABASE `test_recover_db`
            """

                showDatabase = sql """ SHOW CREATE DATABASE `test_recover_db` """
                assertTrue(showDatabase[0][1].contains("CREATE DATABASE `test_recover_db`"))

                sql """
            DROP DATABASE `test_recover_db`
            """

                sql """
            RECOVER DATABASE `test_recover_db` AS `test_recover_db_new`
            """

                showDatabase = sql """ SHOW CREATE DATABASE `test_recover_db_new` """
                assertTrue(showDatabase[0][1].contains("CREATE DATABASE `test_recover_db_new`"))

                sql """
            CREATE TABLE `test_recover_db_new`.`test_recover_tb_2` (
            `k1` int(11) NULL,
            `k2` datetime NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`k1`)
            PARTITION BY RANGE(`k1`)
            (PARTITION p111 VALUES [('-1000'), ('111')),
            PARTITION p222 VALUES [('111'), ('222')),
            PARTITION p333 VALUES [('222'), ('333')),
            PARTITION p1000 VALUES [('333'), ('1000')))
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
            """

                sql """
            RECOVER DATABASE `test_recover_db`
            """    

                sql """
            CREATE TABLE `test_recover_db`.`test_recover_tb_2` (
            `k1` int(11) NULL,
            `k2` datetime NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`k1`)
            PARTITION BY RANGE(`k1`)
            (PARTITION p111 VALUES [('-1000'), ('111')),
            PARTITION p222 VALUES [('111'), ('222')),
            PARTITION p333 VALUES [('222'), ('333')),
            PARTITION p1000 VALUES [('333'), ('1000')))
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            )
            """

                showDatabase = sql """ SHOW CREATE DATABASE `test_recover_db` """
                assertTrue(showDatabase[0][1].contains("CREATE DATABASE `test_recover_db`"))
                showDatabase = sql """ SHOW CREATE DATABASE `test_recover_db_new` """
                assertTrue(showDatabase[0][1].contains("CREATE DATABASE `test_recover_db_new`"))
                res = sql """SHOW CREATE TABLE `test_recover_db`.`test_recover_tb`"""
                assertTrue(res.size() != 0)
                qt_select """SHOW TABLES FROM `test_recover_db`"""
                qt_select """SHOW TABLES FROM `test_recover_db_new`"""

            // test drop many times
                sql """ DROP DATABASE `test_recover_db` """
                sql """ DROP DATABASE `test_recover_db_new` """
        }
    } finally {
        sql """ DROP DATABASE IF EXISTS `test_recover_db` FORCE """
        sql """ DROP DATABASE IF EXISTS `test_recover_db_new` FORCE """
    }

}
