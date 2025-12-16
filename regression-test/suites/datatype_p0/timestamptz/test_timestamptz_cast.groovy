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


suite("test_timestamptz_cast") {

    sql " set time_zone = '+08:00'; "

    sql " set debug_skip_fold_constant = false; "
    qt_cast_from_string0 """
    select cast("2020-01-01 00:00:00.123456+08:00" as timestamptz(5));
    """
    qt_cast_from_string1 """
    select cast("2020-01-01 23:59:59.999999+08:00" as timestamptz(5));
    """
    sql " set debug_skip_fold_constant = true; "
    qt_cast_from_string2 """
    select cast("2020-01-01 00:00:00.123456+08:00" as timestamptz(5));
    """
    qt_cast_from_string3 """
    select cast("2020-01-01 23:59:59.999999+08:00" as timestamptz(5));
    """

    sql """
        DROP TABLE IF EXISTS `timestamptz_cast_test_table_1`;
    """

    sql """
        DROP TABLE IF EXISTS `timestamptz_cast_test_table_2`;
    """
   
    sql """
        CREATE TABLE timestamptz_cast_test_table_1 (id INT, tz timestamptz(3)) DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES ("replication_num" = "1");
    """

    sql """
        CREATE TABLE timestamptz_cast_test_table_2 (id INT, tz timestamptz(5)) DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES ("replication_num" = "1");
    """

    sql """
        insert into timestamptz_cast_test_table_1 values 
        (1, cast("2020-01-01 00:00:00.123 +03:00" as timestamptz(3))),
        (2, cast("2020-06-01 12:00:00.456 +05:00" as timestamptz(3))) , 
        (3, cast("2019-12-31 23:59:59.789 +00:00" as timestamptz(3)));
    """

    sql """
        insert into timestamptz_cast_test_table_2 values 
        (1, cast("2020-01-01 00:00:00.12345 +03:00" as timestamptz(5))),
        (2, cast("2020-06-01 12:00:00.45678 +05:00" as timestamptz(5))) , 
        (3, cast("2019-12-31 23:59:59.78901 +00:00" as timestamptz(5)));
    """


    qt_cast_1 """
        select id, tz, cast(tz as timestamptz(5)) as tz_cast_5 from timestamptz_cast_test_table_1 order by id;
    """

    qt_cast_2 """
        select id, tz, cast(tz as timestamptz(3)) as tz_cast_3 from timestamptz_cast_test_table_2 order by id;
    """

}
