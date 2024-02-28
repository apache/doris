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

suite("test_agg_foreach") {
    sql """ set enable_nereids_planner=false;"""
    sql """  set enable_fallback_to_original_planner=true;"""
   
    sql """
        drop table if exists table_with_all_null;
    """
    sql """
    CREATE TABLE IF NOT EXISTS table_with_all_null (
              `id` INT(11) NULL COMMENT "",
              `a` array<INT> NULL COMMENT "",
              `s` array<String>  NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """
    sql """
       insert into table_with_all_null values
        (1,null,null),
        (2,null,null),
        (3,null,null);
    """

    qt_sql """ 
      select sum_foreach(a) ,  count_foreach(a) , group_concat_foreach(s)   from table_with_all_null;
    """



    sql """
        drop table if exists table_with_all_not_null;
    """

     sql """
        CREATE TABLE IF NOT EXISTS table_with_all_not_null (
              `id` INT(11)  COMMENT "",
              `a` array<INT>  COMMENT "",
              `s` array<String>   COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """

    sql """
    insert into table_with_all_not_null values(1,[1,2,3],["ab","123"]),(2,[20],["cd"]),(3,[100],["efg"]);
    """


    qt_sql """
       select sum_foreach(a) ,  count_foreach(a) , group_concat_foreach(s)  
        from table_with_all_not_null;
    """


    sql """
       drop table if exists table_with_null;
    """

    sql """
       CREATE TABLE IF NOT EXISTS table_with_null (
              `id` INT(11)  COMMENT "",
              `a` array<INT>  COMMENT "",
              `s` array<String>   COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """
    sql """
   insert into table_with_null values(1,[1,2,3],["ab","123"]),(2,[20],["cd"]),(3,[100],["efg"]) , (4,null,null),(5,[null,2],[null,'c']);
   """


    qt_sql """
       select sum_foreach(a) ,  count_foreach(a) , group_concat_foreach(s)  from table_with_null;
    """


    // for nereids_planner

    sql """ set enable_nereids_planner=true;"""
    sql """  set enable_fallback_to_original_planner=false;"""
   
    sql """
        drop table if exists table_with_all_null;
    """
    sql """
    CREATE TABLE IF NOT EXISTS table_with_all_null (
              `id` INT(11) NULL COMMENT "",
              `a` array<INT> NULL COMMENT "",
              `s` array<String>  NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """
    sql """
       insert into table_with_all_null values
        (1,null,null),
        (2,null,null),
        (3,null,null);
    """

    qt_sql """ 
      select sum_foreach(a) ,  count_foreach(a) , group_concat_foreach(s)   from table_with_all_null;
    """

    sql """
        drop table if exists table_with_all_not_null;
    """

     sql """
        CREATE TABLE IF NOT EXISTS table_with_all_not_null (
              `id` INT(11)  COMMENT "",
              `a` array<INT>  COMMENT "",
              `s` array<String>   COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """
    
    sql """
    insert into table_with_all_not_null values(1,[1,2,3],["ab","123"]),(2,[20],["cd"]),(3,[100],["efg"]);
    """

    qt_sql """
       select sum_foreach(a) ,  count_foreach(a) , group_concat_foreach(s)  
        from table_with_all_not_null;
    """

    sql """
       drop table if exists table_with_null;
    """

    sql """
       CREATE TABLE IF NOT EXISTS table_with_null (
              `id` INT(11)  COMMENT "",
              `a` array<INT>  COMMENT "",
              `s` array<String>   COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """
    sql """
    insert into table_with_null values(1,[1,2,3],["ab","123"]),(2,[20],["cd"]),(3,[100],["efg"]) , (4,null,null),(5,[null,2],[null,'c']);
   """


    qt_sql """
       select sum_foreach(a) ,  count_foreach(a) , group_concat_foreach(s) from table_with_null;
    """

}
