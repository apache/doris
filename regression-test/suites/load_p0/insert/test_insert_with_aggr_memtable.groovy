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

suite("test_insert_with_aggr_memtable") {
    // enable memtable
    sql """ set enable_memtable_on_sink_node=true """
    sql """ set experimental_enable_pipeline_x_engine=false """
    // start load
    sql "show load"
    def insert_tbl = "test_insert_with_aggr_memtable_tbl";

    sql """ DROP TABLE IF EXISTS ${insert_tbl}"""
    // create table with aggreate key
    sql """
        CREATE TABLE ${insert_tbl} (
                `id` LARGEINT NOT NULL, 
                `k1` DATE NOT NULL, 
                `k2` VARCHAR(20),
                `k3` SMALLINT,
                `k4` TINYINT,
                `k5` DATETIME REPLACE DEFAULT '1970-01-01 00:00:00',
                `k6` BIGINT SUM DEFAULT '0',
                `k7` INT MAX DEFAULT '0',
                `k8` INT MIN DEFAULT '99999'
        ) ENGINE=OLAP
        AGGREGATE KEY(`id`, `k1`, `k2`, `k3`, `k4`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5
        PROPERTIES (
            'replication_num'='1'
        );
    """
    sql "sync"
    // insert some data for test
    sql """insert into ${insert_tbl} values
        (10000,"2017-10-01",'北京',20,0,"2017-10-01 06:00:00",20,10,10),
        (10000,"2017-10-01",'北京',20,0,"2017-10-01 07:00:00",15,2,2),
        (10001,"2017-10-01",'北京',30,1,"2017-10-01 17:05:45",2,22,22),
        (10002,"2017-10-02",'上海',20,1,"2017-10-02 12:59:12",200,5,5),
        (10003,"2017-10-02",'广州',32,0,"2017-10-02 11:20:00",30,11,11),
        (10004,"2017-10-01",'深圳',35,0,"2017-10-01 10:00:15",100,3,3),
        (10004,"2017-10-03",'深圳',35,0,"2017-10-03 10:20:22",11,6,6);"""

    sql "sync"
    // check
    qt_select """ select * from ${insert_tbl} order by id """
    sql """ set enable_memtable_on_sink_node=false """
}
