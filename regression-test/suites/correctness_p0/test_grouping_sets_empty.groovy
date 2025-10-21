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

suite("test_grouping_sets_empty") {

    sql"""
        drop table if exists test_grouping_sets_empty;
    """

    sql"""
        create table test_grouping_sets_empty (a int) distributed by hash(a) buckets 1 properties ( 'replication_num' = '1');
    """

    sql """
        insert into test_grouping_sets_empty values (1),(2),(3);
    """


    sql """ 
        set experimental_enable_pipeline_x_engine=true;
    """

    qt_select1 """
        select count(a) from test_grouping_sets_empty group by grouping sets (());
    """


    qt_select2 """
        select count(*) from test_grouping_sets_empty group by grouping sets (());
    """

    qt_select3 """
        select count(*) from test_grouping_sets_empty group by grouping sets ((),(),());
    """

    qt_select4 """
        select a from test_grouping_sets_empty group by grouping sets ((),(),(),(a)) order by a;
    """


    qt_select5 """
        select count(a) from test_grouping_sets_empty group by grouping sets (());
    """


    qt_select6 """
        select count(*) from test_grouping_sets_empty group by grouping sets (());
    """

    qt_select7 """
        select count(*) from test_grouping_sets_empty group by grouping sets ((),(),());
    """

    qt_select8 """
        select a from test_grouping_sets_empty group by grouping sets ((),(),(),(a)) order by a;
    """



}
