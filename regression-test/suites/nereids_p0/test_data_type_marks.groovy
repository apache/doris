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

suite("test_data_type_marks") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
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
}
