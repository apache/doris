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


suite("test_timestamptz_group_by") {

    sql " set time_zone = '+08:00'; "

    sql """
        DROP TABLE IF EXISTS `test_timestamptz_group_by`;
    """

    sql """
        CREATE TABLE test_timestamptz_group_by (id INT, tz timestamptz) DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES ("replication_num" = "1");
    """


    sql """
        insert into test_timestamptz_group_by values 
        (1, cast("2020-01-01 00:00:00 +03:00" as timestamptz)),
        (2, cast("2020-06-01 12:00:00 +05:00" as timestamptz)) , 
        (3, cast("2019-12-31 23:59:59 +00:00" as timestamptz)),
        (4, cast("2020-01-01 00:00:00 +03:00" as timestamptz));
    """


    qt_sql """
        SELECT tz, COUNT(*) FROM test_timestamptz_group_by GROUP BY tz ORDER BY tz;
    """


}
