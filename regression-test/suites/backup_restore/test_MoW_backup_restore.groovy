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

suite("test_MoW_backup_restore") {
    String s3_url = ""
    String ak = ""
    String sk = ""
    String end_point = ""
    String region = ""


    qt_1 """create repository `demo_MoW_1` with s3 on location \"""" + s3_url + """\" properties ("AWS_ACCESS_KEY" =\"""" + ak + """\","AWS_SECRET_KEY" =\"""" + sk + """\","AWS_ENDPOINT" =\"""" + end_point + """\","AWS_REGION" =\"""" + region+"""\")"""
    qt_2 """show repositories"""

    sql """drop table if exists demo_MoW"""
    sql """CREATE TABLE IF NOT EXISTS demo_MoW 
    ( `user_id` INT NOT NULL, `value` INT NOT NULL)
    UNIQUE KEY(`user_id`) 
    DISTRIBUTED BY HASH(`user_id`) 
    BUCKETS 1 
    PROPERTIES ("replication_allocation" = "tag.location.default: 1",
    "disable_auto_compaction" = "true",
    "enable_unique_key_merge_on_write" = "true");"""

    // version1 (1,1)(2,2)
    sql """insert into demo_MoW values(1,1),(2,2)"""
    sql """backup snapshot test.snapshot1 to s3_demo_MoW_1 on (demo_MoW) properties("type"="full")"""
    qt_3 """select * from demo_MoW"""

    // version2 (1,10)(2,2)
    sql """insert into demo_MoW values(1,10)"""
    sql """backup snapshot test.snapshot2 to s3_demo_MoW_1 on (demo_MoW) properties("type"="full")"""
    qt_4 """select * from demo_MoW"""

    // version3 (1,100)(2,2)
    sql """update demo_MoW set value = 100 where user_id = 1"""
    sql """backup snapshot test.snapshot3 to s3_demo_MoW_1 on (demo_MoW) properties("type"="full")"""
    qt_5 """select * from demo_MoW"""

    // version4 (2,2)
    sql """delete from demo_MoW where user_id = 1"""
    sql """backup snapshot test.snapshot4 to s3_demo_MoW_1 on (demo_MoW) properties("type"="full")"""
    qt_6 """select * from demo_MoW"""

    // version1 (1,1)(2,2)
    sql """restore snapshot test.snapshot1 from `s3_demo_MoW_1` on(`demo_MoW`)PROPERTIES ( "backup_timestamp"="2023-06-20-17-54-10","replication_num" = "1")"""
    qt_7 """select * from demo_MoW"""
    // version2 (1,10)(2,2)
    sql """restore snapshot test.snapshot2 from `s3_demo_MoW_1` on(`demo_MoW`)PROPERTIES ( "backup_timestamp"="2023-06-20-17-54-41","replication_num" = "1")"""
    qt_8 """select * from demo_MoW"""
    // version3 (1,100)(2,2)
    sql """restore snapshot test.snapshot3 from `s3_demo_MoW_1` on(`demo_MoW`)PROPERTIES ( "backup_timestamp"="2023-06-20-17-55-03","replication_num" = "1")"""
    qt_9 """select * from demo_MoW"""
    // version4 (2,2)
    sql """restore snapshot test.snapshot4 from `s3_demo_MoW_1` on(`demo_MoW`)PROPERTIES ( "backup_timestamp"="2023-06-20-17-55-23","replication_num" = "1")"""
    qt_10 """select * from demo_MoW"""

    sql """drop table if exists demo_MoW"""
    sql """CREATE TABLE IF NOT EXISTS demo_MoW 
    ( `user_id` INT NOT NULL, `value` INT NOT NULL)
    UNIQUE KEY(`user_id`) 
    DISTRIBUTED BY HASH(`user_id`) 
    BUCKETS 1 
    PROPERTIES ("replication_allocation" = "tag.location.default: 1",
    "disable_auto_compaction" = "true",
    "enable_unique_key_merge_on_write" = "true");""" 

    // version1 (1,1)(2,2)
    sql """restore snapshot test.snapshot1 from `s3_demo_MoW_1` on(`demo_MoW`)PROPERTIES ( "backup_timestamp"="2023-06-20-17-54-10","replication_num" = "1")"""
    qt_11 """select * from demo_MoW"""
    // version2 (1,10)(2,2)
    sql """restore snapshot test.snapshot2 from `s3_demo_MoW_1` on(`demo_MoW`)PROPERTIES ( "backup_timestamp"="2023-06-20-17-54-41","replication_num" = "1")"""
    qt_12 """select * from demo_MoW"""
    // version3 (1,100)(2,2)
    sql """restore snapshot test.snapshot3 from `s3_demo_MoW_1` on(`demo_MoW`)PROPERTIES ( "backup_timestamp"="2023-06-20-17-55-03","replication_num" = "1")"""
    qt_13 """select * from demo_MoW"""
    // version4 (2,2)
    sql """restore snapshot test.snapshot4 from `s3_demo_MoW_1` on(`demo_MoW`)PROPERTIES ( "backup_timestamp"="2023-06-20-17-55-23","replication_num" = "1")"""
    qt_14 """select * from demo_MoW"""
}