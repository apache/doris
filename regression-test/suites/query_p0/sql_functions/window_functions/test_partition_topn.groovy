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

suite("test_partition_topn") {
    sql """ DROP TABLE IF EXISTS test_partition_topn """
    sql """
        CREATE TABLE IF NOT EXISTS test_partition_topn (
            u_id int NULL COMMENT "",
            u_city varchar(20) NULL COMMENT "",
            u_salary int NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`u_id`, `u_city`, `u_salary`)
        DISTRIBUTED BY HASH(`u_id`, `u_city`, `u_salary`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
    );
    """

    sql """
        INSERT INTO test_partition_topn(u_id, u_city, u_salary) VALUES
        ('1',  'gz', 30000),
        ('2',  'gz', 25000),
        ('3',  'gz', 17000),
        ('4',  'gz', 32000),
        ('5',  'gz', 30000),
        ('6',  'gz', 25000),
        ('7',  'gz', 17000),
        ('8',  'gz', 32000),
        ('9',  'gz', 32000),
        ('10',  'gz', 32000),
        ('11',  'gz', 32000),
        ('12',  'sz', 30000),
        ('13',  'sz', 25000),
        ('14',  'sz', 17000),
        ('15',  'sz', 32000),
        ('16',  'sz', 30000),
        ('16',  'sz', 25000),
        ('17',  'sz', 17000),
        ('18',  'sz', 32000),
        ('19',  'sz', 32000),
        ('20',  'sz', 32000),
        ('21',  'sz', 32000);
     """

    sql """ set parallel_pipeline_task_num = 1; """

    qt_sql_topn """ select * from (select u_id, row_number() over(partition by u_city order by u_id) as rn from test_partition_topn)t where rn = 1 order by 1; """
}


