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

suite("aggregate_function_kurt") {
    sql """
        drop table if exists aggregate_function_kurt;
    """
    sql"""
       create table aggregate_function_kurt (tag int, val1 double not null, val2 double null) distributed by hash(tag) buckets 10 properties('replication_num' = '1');
    """

    qt_sql_empty_1 """
        select kurtosis(val1),kurtosis(val2) from aggregate_function_kurt;
    """
    qt_sql_empty_2 """
        select kurtosis(val1),kurtosis(val2) from aggregate_function_kurt group by tag;
    """

    sql """
       insert into aggregate_function_kurt values (1, -10.0, -10.0);
    """

    qt_sql_1 """
        select kurtosis(val1),kurtosis(val2) from aggregate_function_kurt;
    """
    qt_sql_2 """
        select kurtosis(val1),kurtosis(val2) from aggregate_function_kurt group by tag;
    """

    sql """
       insert into aggregate_function_kurt values (2, -20.0, NULL), (3, 100, NULL), (4, 100, 100), (5,1000, 1000);
    """
    qt_sql_3 """
        select kurtosis(val1),kurtosis(val2) from aggregate_function_kurt;
    """
    qt_sql_4 """
        select kurtosis(val1),kurtosis(val2) from aggregate_function_kurt group by tag;
    """

    qt_sql_distinct_1 """
        select kurtosis(distinct val1) from aggregate_function_kurt;
    """
    qt_sql_distinct_2 """
        select kurtosis(distinct val2) from aggregate_function_kurt;
    """

    qt_sql_distinct_3 """
        select kurtosis(distinct val1) from aggregate_function_kurt group by tag;
    """
    qt_sql_distinct_4 """
        select kurtosis(distinct val2) from aggregate_function_kurt group by tag;
    """

    sql """
        insert into aggregate_function_kurt select * from aggregate_function_kurt;
    """

    qt_sql_5 """
        select kurt(val1),kurt_pop(val2) from aggregate_function_kurt;
    """
    qt_sql_6 """
        select kurt(val1),kurt_pop(val2) from aggregate_function_kurt group by tag;
    """
}