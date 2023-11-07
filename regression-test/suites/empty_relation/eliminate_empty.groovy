/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("eliminate_empty") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set forbid_unknown_col_stats=false'
    qt_onerow_union """
        select * from (select 1, 2 union select 3, 4) T
    """

    qt_join """
        explain shape plan
        select * 
        from 
            nation 
            join 
            (select * from region where false) R
    """

    qt_explain_union_empty_data """
        explain shape plan
        select * 
        from (select n_nationkey from nation union select r_regionkey from region where false) T
    """
    qt_union_empty_data """
        select * 
        from (select n_nationkey from nation union select r_regionkey from region where false) T
    """

    qt_explain_union_empty_empty """
        explain shape plan
        select * 
        from (
                select n_nationkey from nation where false 
                union 
                select r_regionkey from region where false
            ) T
    """
    qt_union_empty_empty """
        select * 
        from (
                select n_nationkey from nation where false 
                union 
                select r_regionkey from region where false
            ) T
    """
    qt_union_emtpy_onerow """
        select *
        from (
            select n_nationkey from nation where false 
                union
            select 10
                union
            select 10
        )T
        """

    qt_explain_intersect_data_empty """
        explain shape plan
        select n_nationkey from nation intersect select r_regionkey from region where false
    """

    qt_explain_intersect_empty_data """
        explain shape plan
        select r_regionkey from region where false intersect select n_nationkey from nation  
    """

    qt_explain_except_data_empty """
        explain shape plan
        select n_nationkey from nation except select r_regionkey from region where false
    """

    qt_explain_except_data_empty_data """
        explain shape plan
        select n_nationkey from nation 
        except 
        select r_regionkey from region where false
        except
        select n_nationkey from nation where n_nationkey != 1;
    """

    qt_except_data_empty_data """
        select n_nationkey from nation 
        except 
        select r_regionkey from region where false
        except
        select n_nationkey from nation where n_nationkey != 1;
    """

    qt_explain_except_empty_data """
        explain shape plan
        select r_regionkey from region where false except select n_nationkey from nation  
    """
    

    qt_intersect_data_empty """
        select n_nationkey from nation intersect select r_regionkey from region where false
    """

    qt_intersect_empty_data """
        select r_regionkey from region where false intersect select n_nationkey from nation  
    """

    qt_except_data_empty """
        select n_nationkey from nation except select r_regionkey from region where false
    """

    qt_except_empty_data """
        select r_regionkey from region where false except select n_nationkey from nation  
    """

    sql """
    drop table if exists eliminate_partition_prune;
    """
    sql """
    CREATE TABLE `eliminate_partition_prune` (
    `k1` int(11) NULL COMMENT "",
    `k2` int(11) NULL COMMENT "",
    `k3` int(11) NULL COMMENT ""
    ) 
    PARTITION BY RANGE(`k1`, `k2`)
    (PARTITION p1 VALUES LESS THAN ("3", "1"),
    PARTITION p2 VALUES [("3", "1"), ("7", "10")),
    PARTITION p3 VALUES [("7", "10"), ("10", "15")))
    DISTRIBUTED BY HASH(`k1`) BUCKETS 10
    PROPERTIES ('replication_num' = '1');
    """

    
    qt_prune_partition1 """
        explain shape plan
        select sum(k2)
        from
        (select * from eliminate_partition_prune where k1=100) T
        group by k3;
        """
    sql """
        insert into eliminate_partition_prune values (7, 0, 0)
        """
    qt_prune_partition2 """
        explain shape plan
        select sum(k2)
        from
        (select * from eliminate_partition_prune where k1=100) T
        group by k3;
        """
}