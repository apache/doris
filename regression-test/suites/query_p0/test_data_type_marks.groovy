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

suite("test_data_type_marks", "arrow_flight_sql") {
    def tbName = "org"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                  `id` bigint(20) NULL COMMENT "主键id",
                  `tenant_id` bigint(20) NULL COMMENT "租户ID",
                  `name` text NULL COMMENT "名称"
                ) ENGINE=OLAP
                UNIQUE KEY(`id`)
                COMMENT "OLAP"
                DISTRIBUTED BY HASH(`id`) BUCKETS 8
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "in_memory" = "false",
                "storage_format" = "V2"
                )
        """
    sql "insert into ${tbName} values (639215401565159424,1143681147589283841,'test'),(639237839376089088,1143681147589283841,'test123');"

    qt_select_no_marks "select * from org where id in (639215401565159424) and id=639237839376089088;"
    qt_select_one_marks "select * from org where id in ('639215401565159424') ;"
    qt_select_two_marks "select * from org where id in ('639215401565159424') and  id='639237839376089088';"
    sql "DROP TABLE ${tbName}"

    sql "DROP TABLE IF EXISTS a_table"
    sql """
        create table a_table(
            k1 int null,
            k2 int not null,
            k3 bigint null,
            k4 bigint sum null,
            k5 bitmap bitmap_union ,
            k6 hll hll_union 
        )
        aggregate key (k1,k2,k3)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
        """
    sql """insert into a_table select 1,1,1,1,to_bitmap(1),hll_hash(1);"""
    sql """insert into a_table select 2,2,1,2,to_bitmap(2),hll_hash(2);"""
    sql """insert into a_table select 3,-3,-3,-3,to_bitmap(3),hll_hash(3);"""

    qt_sql """select bitmap_count(k5) from a_table where abs(k1)=1;"""
}
