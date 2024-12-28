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

suite("test_iot_overwrite_and_create") {
    sql "set enable_auto_create_when_overwrite = true;"

    sql " drop table if exists auto_list; "
    sql """
        create table auto_list(
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
        DISTRIBUTED BY HASH(`k0`) BUCKETS 1
        properties("replication_num" = "1");
    """
    sql """ insert into auto_list values ("Beijing"),("Shanghai"),("xxx"),("list"),("1234567"); """
    qt_origin "select * from auto_list order by k0;"

    sql """insert overwrite table auto_list values ("SHANGHAI"), ("zzz");"""
    qt_0 "select * from auto_list order by k0;"
    sql """insert overwrite table auto_list values ("zzz2");"""
    qt_1 "select * from auto_list order by k0;"

    test{
        sql """insert overwrite table auto_list partition(p1, p2) values ("zzz");"""
        exception "Insert has filtered data in strict mode."
    }
    test{
        sql """insert overwrite table auto_list partition(p3) values ("zzz3");"""
        exception "Insert has filtered data in strict mode."
    }

    sql """ insert into auto_list values ("Beijing"),("Shanghai"),("xxx"),("list"),("1234567"); """
    sql """insert overwrite table auto_list partition(*) values ("abcd"), ("BEIJING");"""
    qt_2 "select * from auto_list order by k0;"

    sql "set enable_auto_create_when_overwrite = false;"
    test{
        sql """insert overwrite table auto_list values ("zzz3");"""
        exception "Insert has filtered data in strict mode."
    }
    test{
        sql """insert overwrite table auto_list partition(p1, p2) values ("zzz");"""
        exception "Insert has filtered data in strict mode."
    }
    test{
        sql """insert overwrite table auto_list partition(*) values ("zzz3");"""
        exception "Cannot found origin partitions in auto detect overwriting"
    }
}
