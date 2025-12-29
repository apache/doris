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

import java.time.ZonedDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import java.time.temporal.ChronoUnit
suite("test_tz_array_functions") {
    def timezone_str = "+08:00"
    sql "set time_zone = '${timezone_str}'; "

    qt_array """
    select array(cast('2023-08-08 20:20:20.000000 +00:00' as timestamptz(6)), cast('2023-08-08 20:20:20.123456 +00:00' as timestamptz(6)),
                 cast('9999-12-31 23:59:59.999999 +08:00' as timestamptz(6)), cast('0000-01-01 00:00:00.000001 +00:00' as timestamptz(6)));
    """

    sql """
        drop table if exists test_tz_array_functions;
    """
    sql """
        create table test_tz_array_functions (
            id int ,
            array_tz1 array<timestamptz(6)>,
            array_tz2 array<timestamptz(6)>
        )
        properties (
        "replication_num" = "1"
        );
    """

    sql """
        insert into test_tz_array_functions values
        (0, [null, '0000-01-01 00:00:00.123456 +00:00', '0000-01-01 00:00:00.999999 +00:00', '0000-01-01 00:00:00.000001 +00:00', '0000-01-01 00:00:00 +00:00',
             '2023-08-08 20:20:20 +00:00', '2023-08-08 20:20:20.000000 +00:00', '2023-08-08 20:20:20.000001 +00:00', '2023-08-08 20:20:20.123456 +00:00', '2023-08-08 20:20:20.999999 +00:00',
             '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59.000000 +08:00', '9999-12-31 23:59:59.000001 +08:00', '9999-12-31 23:59:59.123456 +08:00', '9999-12-31 23:59:59.999999 +08:00'],
             ['0000-01-01 00:00:00 +00:00','2023-08-08 20:20:20.123456 +00:00','9999-12-31 23:59:59.999999 +08:00']);
    """

    qt_all0 """
        select id, array_sort(array_tz1) from test_tz_array_functions;
    """

    qt_min_max """
        select id, array_min(array_tz1), array_max(array_tz1)  from test_tz_array_functions;
    """
    test {
        sql """
        select array_sum(array_tz1) from test_tz_array_functions;
        """
        exception "does not support"
    }
    test {
        sql """
        select array_avg(array_tz1) from test_tz_array_functions;
        """
        exception "does not support"
    }
    test {
        sql """
        select array_product(array_tz1) from test_tz_array_functions;
        """
        exception "Can not find"
    }
    qt_join """
        select id, array_join(array_sort(array_tz1), "|") from test_tz_array_functions;
    """

    qt_apply_eq0 """
        select id, array_sort(array_apply(array_tz1, "=", '0000-01-01 00:00:00 +00:00')) from test_tz_array_functions;
    """
    qt_apply_eq1 """
        select id, array_sort(array_apply(array_tz1, "=", '2023-08-08 20:20:20.123456 +00:00')) from test_tz_array_functions;
    """

    qt_apply_neq0 """
        select id, array_sort(array_apply(array_tz1, "!=", '0000-01-01 00:00:00 +00:00')) from test_tz_array_functions;
    """

    qt_apply_gt0 """
        select id, array_sort(array_apply(array_tz1, ">", '0000-01-01 00:00:00 +00:00')) from test_tz_array_functions;
    """

    qt_apply_ge0 """
        select id, array_sort(array_apply(array_tz1, ">=", '0000-01-01 00:00:00.123456 +00:00')) from test_tz_array_functions;
    """

    qt_apply_lt0 """
        select id, array_sort(array_apply(array_tz1, "<", '9999-12-31 23:59:59.000001 +08:00')) from test_tz_array_functions;
    """

    qt_apply_le0 """
        select id, array_sort(array_apply(array_tz1, "<=", '9999-12-31 23:59:59.000001 +08:00')) from test_tz_array_functions;
    """

    // array_sort(array_except(CAST(array_tz1[#1] AS array<text>), ['0000-01-01 00:00:00 +00:00', '2023-08-08 20:20:20.123456 +00:00', '9999-12-31 23:59:59.999999 +08:00']))
    // select id, array_sort(array_except(array_tz1, ['0000-01-01 00:00:00 +00:00','2023-08-08 20:20:20.123456 +00:00','9999-12-31 23:59:59.999999 +08:00'])) from test_tz_array_functions;
    qt_apply_except """
        select id, array_sort(array_except(array_tz1, array_tz2)) from test_tz_array_functions;
    """

}