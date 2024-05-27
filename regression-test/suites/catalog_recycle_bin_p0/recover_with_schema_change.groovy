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

suite("recover_with_schema_change") {
    def tableName = "recover_with_schema_change"

    for (int i = 0; i <= 4; i++) {
        def table = tableName + i

        // create table and insert data
        sql """ drop table if exists ${table} """
        sql """
        create table ${table} (
            `id` int(11),
            `name` varchar(128),
            `da` date
        )
        engine=olap
        duplicate key(id)
        partition by range(da)(
            PARTITION p3 VALUES LESS THAN ('2023-01-01'),
            PARTITION p4 VALUES LESS THAN ('2024-01-01'),
            PARTITION p5 VALUES LESS THAN ('2025-01-01')
        )
        distributed by hash(id) buckets 2
        properties(
            "replication_num"="1",
            "light_schema_change"="true"
        );
        """

        sql """ insert into ${table} values(1, 'a', '2022-01-02'); """
        sql """ insert into ${table} values(2, 'a', '2023-01-02'); """
        sql """ insert into ${table} values(3, 'a', '2024-01-02'); """

        if (i == 3 || i == 4) {
            // create mv
            createMV """ create materialized view mv_${table} as select name from ${table}; """
        }

        // drop partition
        sql """ ALTER TABLE ${table} DROP PARTITION p3; """

        if (i == 0) {
            // do light weight schema change
            sql """ ALTER TABLE ${table} add column `age` int after name; """
            waitForSchemaChangeDone {
                sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${table}' ORDER BY createtime DESC LIMIT 1 """
                time 60
            }

            // recover partition should success
            sql """ recover partition p3 from regression_test_catalog_recycle_bin_p0.${table}; """
        } else if (i == 1) {
            // do hard weight schema change
            sql """ ALTER TABLE ${table} order by(`id`, `da`, `name`); """
            waitForSchemaChangeDone {
                sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${table}' ORDER BY createtime DESC LIMIT 1 """
                time 60
            }

            // recover partition should fail
            test {
                sql """ recover partition p3 from regression_test_catalog_recycle_bin_p0.${table}; """
                exception "table's index not equal with partition's index"
            }
        } else if (i == 2) {
            // create mv
            createMV """ create materialized view mv_${table} as select name from ${table}; """

            // recover partition should fail
            test {
                sql """ recover partition p3 from regression_test_catalog_recycle_bin_p0.${table}; """
                exception "table's index not equal with partition's index"
            }
        } else if (i == 3) {
            // drop mv
            sql """ drop materialized view mv_${table} on ${table}; """

            // recover partition should fail
            test {
                sql """ recover partition p3 from regression_test_catalog_recycle_bin_p0.${table}; """
                exception "table's index not equal with partition's index"
            }
        } else if (i == 4) {
            // drop mv
            sql """ drop materialized view mv_${table} on ${table}; """

            // create mv
            createMV """ create materialized view mv_${table} as select name from ${table}; """

            // recover partition should fail
            test {
                sql """ recover partition p3 from regression_test_catalog_recycle_bin_p0.${table}; """
                exception "table's index not equal with partition's index"
            }
        }

        // write data
        if (i == 0) {
            sql """ insert into ${table} values(4, 'a', 10, '2022-01-02'); """
        } else {
            test {
                sql """ insert into ${table} values(4, 'b', '2022-01-02'); """
                exception ""
            }
            order_qt_sql """ select name from ${table}; """
        }

        // read data
        order_qt_sql """ select * from ${table}; """
    }
}
