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


suite("test_timestamptz_complext_type") {

    sql " set time_zone = '+08:00'; "

    qt_sql_array """
        select 
            array(
                cast("2020-01-01 00:00:00 +03:00" as timestamptz), 
                cast("2020-06-01 12:00:00 +05:00" as timestamptz), 
                cast("2019-12-31 23:59:59 +00:00" as timestamptz)
            ) as tz_array;
    """


    qt_sql_map """
        select 
            map(
                1, cast("2020-01-01 00:00:00 +03:00" as timestamptz), 
                2, cast("2020-06-01 12:00:00 +05:00" as timestamptz), 
                3, cast("2019-12-31 23:59:59 +00:00" as timestamptz)
            ) as tz_map;
    """


    qt_sql_struct """
        select 
            named_struct(
                'first', cast("2020-01-01 00:00:00 +03:00" as timestamptz), 
                'second', cast("2020-06-01 12:00:00 +05:00" as timestamptz), 
                'third', cast("2019-12-31 23:59:59 +00:00" as timestamptz)
            ) as tz_struct;
    """ 


    qt_parse_array """
        select 
            cast('["2020-01-01 00:00:00 +03:00", "2020-06-01 12:00:00 +05:00", "2019-12-31 23:59:59 +00:00"]' as array<timestamptz>) as tz_array;
    """

    qt_parse_map """
        select 
            cast('{"1": "2020-01-01 00:00:00 +03:00", "2": "2020-06-01 12:00:00 +05:00", "3": "2019-12-31 23:59:59 +00:00"}' as map<int, timestamptz>) as tz_map;
    """

    qt_parse_struct """
        select 
            cast('{"first": "2020-01-01 00:00:00 +03:00", "second": "2020-06-01 12:00:00 +05:00", "third": "2019-12-31 23:59:59 +00:00"}' as struct<first:timestamptz, second:timestamptz, third:timestamptz>) as tz_struct;
    """

}
