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
suite("window_agg_grouping_sets") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """
         DROP TABLE IF EXISTS test_window_table2
        """

    sql """
        create table test_window_table2 ( a varchar(100) null, b decimalv3(18,10) null, c int, ) ENGINE=OLAP 
        DUPLICATE KEY(`a`) DISTRIBUTED BY HASH(`a`) BUCKETS 1 PROPERTIES 
        ( "replication_allocation" = "tag.location.default: 1" ); 
         """

    sql """
        insert into test_window_table2 values("1", 1, 1),("1", 1, 2),("1", 2, 1),("1", 2, 2),
        ("2", 11, 1),("2", 11, 2),("2", 12, 1),("2", 12, 2);
        """

    qt_select1 """
        select a, c, sum(sum(b)) over(partition by c order by c rows between unbounded preceding and current row) 
        from test_window_table2 group by grouping sets((a),( c)) having a > 1 order by 1,2,3;
     """

}
