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

suite("test_char_default_value") {
    sql """
        drop table if exists test_char_default_value;
    """
    
    sql """
        CREATE TABLE `test_char_default_value` (
            `a` int(11) NULL,
            `b` char(10) NULL DEFAULT "ss",
            `c` varchar(10) NULL DEFAULT "ass"
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`, `b`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`a`) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        insert into test_char_default_value(a) values(1);
    """

    sql """ """
    sql """ insert into test_char_default_value(a) values(2); """
    sql """ insert into test_char_default_value(a) values(3); """
    sql """ insert into test_char_default_value(a,b) values(4,'aaaaaa'); """
    sql """ insert into test_char_default_value(a) values(5); """
    sql """ insert into test_char_default_value(a,b) values(6,'aaaaaaaa'); """
    sql """ insert into test_char_default_value(a,b) values(7,'aaaaaaaa1'); """
    sql """ insert into test_char_default_value(a,b,c) values(8,'aaaaaaaa1','a'); """

    qt_select """
        select
        a,b,c,length(b),length(c)
        from
        test_char_default_value order by a desc;
    """

    sql """
        drop table if exists test_char_default_value;
    """
}
