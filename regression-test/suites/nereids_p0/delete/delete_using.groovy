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

suite('nereids_delete_using') {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
            sql "use ${db};"

            sql 'drop table if exists t1'
            sql """
                create table t1 (
                    id int,
                    dt date,
                    c1 bigint,
                    c2 string,
                    c3 double
                ) unique key (id, dt)
                partition by range(dt) (
                    from ("2000-01-01") TO ("2000-01-31") INTERVAL 1 DAY
                )
                distributed by hash(id)
                properties(
                    'replication_num'='1',
                    "enable_unique_key_merge_on_write" = "true",
                    "store_row_column" = "${use_row_store}"
                );
            """

            sql 'drop table if exists t2'
            sql '''
                create table t2 (
                    id int,
                    dt date,
                    c1 bigint,
                    c2 string,
                    c3 double
                ) unique key (id)
                distributed by hash(id)
                properties(
                    'replication_num'='1'
                );
            '''

            sql 'drop table if exists t3'
            sql '''
                create table t3 (
                    id int
                ) distributed by hash(id)
                properties(
                    'replication_num'='1'
                );
            '''

            sql '''
                INSERT INTO t1 VALUES
                    (1, '2000-01-01', 1, '1', 1.0),
                    (2, '2000-01-02', 2, '2', 2.0),
                    (3, '2000-01-03', 3, '3', 3.0);
            '''

            sql '''

                INSERT INTO t2 VALUES
                    (1, '2000-01-10', 10, '10', 10.0),
                    (2, '2000-01-20', 20, '20', 20.0),
                    (3, '2000-01-30', 30, '30', 30.0),
                    (4, '2000-01-04', 4, '4', 4.0),
                    (5, '2000-01-05', 5, '5', 5.0);
            '''

            sql '''
                INSERT INTO t3 VALUES
                    (1),
                    (2),
                    (4),
                    (5);
            '''
            
            sql 'set enable_nereids_planner=true'
            sql 'set enable_fallback_to_original_planner=false'
            sql 'set enable_nereids_dml=true'

            qt_original_data 'select * from t1 order by id, dt'

            test {
                sql '''
                    delete from t1 temporary partition (p_20000102)
                    using t2 join t3 on t2.id = t3.id
                    where t1.id = t2.id;
                '''
                exception 'Partition: p_20000102 is not exists'
            }

            sql '''
                delete from t1 partition (p_20000102)
                using t2 join t3 on t2.id = t3.id
                where t1.id = t2.id;
            '''

            qt_after_delete_from_partition 'select * from t1 order by id, dt'

            sql '''
                delete from t1
                using t2 join t3 on t2.id = t3.id
                where t1.id = t2.id;
            '''

            qt_after_delete 'select * from t1 order by id, dt'
        }
    }
}
