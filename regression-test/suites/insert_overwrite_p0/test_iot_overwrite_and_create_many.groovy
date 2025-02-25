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

suite("test_iot_overwrite_and_create_many") {
    sql "set enable_auto_create_when_overwrite = true;"

    sql " drop table if exists target; "
    sql """
        create table target(
            k0 varchar null
        )
        auto partition by list (k0)
        (
            PARTITION p1 values in (("Beijing"), ("BEIJING")),
            PARTITION p2 values in (("Shanghai"), ("SHANGHAI")),
            PARTITION p3 values in (("xxx"), ("XXX")),
            PARTITION p4 values in (("list"), ("LIST")),
            PARTITION p5 values in (("1234567"), ("7654321"))
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 2
        properties("replication_num" = "1");
    """
    sql """ insert into target values ("Beijing"),("Shanghai"),("xxx"),("list"),("1234567"); """

    sql " drop table if exists source; "
    sql """
        create table source(
            k0 varchar null
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 10
        properties("replication_num" = "1");
    """

    sql """ insert into source select "Beijing" from numbers("number" = "20000"); """
    sql """ insert into source select "Shanghai" from numbers("number" = "20000"); """
    sql """ insert into source select "zzz" from numbers("number"= "20000"); """
    def result
    result = sql " show partitions from target; "
    logger.info("origin: ${result}")

    sql " insert overwrite table target partition(*) select * from source; "
    result = sql " show partitions from target; "
    logger.info("changed: ${result}")

    qt_sql1 " select k0, count(k0) from target group by k0 order by k0; "

    sql """ insert into source select "yyy" from numbers("number" = "20000"); """
    sql " insert overwrite table target select * from source; "
    qt_sql2 " select k0, count(k0) from target group by k0 order by k0; "
}
