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

suite("test_nereids_null_aware_left_anti_join") {
    def tableName1 = "test_null_aware_left_anti_join1"
    def tableName2 = "test_null_aware_left_anti_join2"
    sql """
        drop table if exists ${tableName1};
    """

    sql """
        drop table if exists ${tableName2};
    """

    sql """
        create table if not exists ${tableName1} ( `k1` int(11) NULL ) DISTRIBUTED BY HASH(`k1`) BUCKETS 4         PROPERTIES (         "replication_num" = "1");
    """

    sql """
        create table if not exists ${tableName2} ( `k1` int(11) NULL ) DISTRIBUTED BY HASH(`k1`) BUCKETS 4         PROPERTIES (         "replication_num" = "1");
    """

    sql """
        insert into ${tableName1} values (1), (3);
    """

    sql """
        insert into ${tableName2} values (1), (2);
    """

    qt_select """ select ${tableName2}.k1 from ${tableName2} where k1 not in (select ${tableName1}.k1 from ${tableName1}) order by ${tableName2}.k1; """

    sql """
        insert into ${tableName2} values(null);
    """

    qt_select """ select ${tableName2}.k1 from ${tableName2} where k1 not in (select ${tableName1}.k1 from ${tableName1}) order by ${tableName2}.k1; """

    sql """
        insert into ${tableName1} values(null);
    """

    qt_select """ select ${tableName2}.k1 from ${tableName2} where k1 not in (select ${tableName1}.k1 from ${tableName1}) order by ${tableName2}.k1; """

    sql """ set parallel_fragment_exec_instance_num=2; """
    sql """ set parallel_pipeline_task_num=2; """
    qt_select """ select ${tableName2}.k1 from ${tableName2} where k1 not in (select ${tableName1}.k1 from ${tableName1}) order by ${tableName2}.k1; """

    sql """
        drop table if exists ${tableName2};
    """

    sql """
        drop table if exists ${tableName1};
    """
}
