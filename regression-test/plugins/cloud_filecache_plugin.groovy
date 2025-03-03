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
import groovy.json.JsonOutput
import org.apache.doris.regression.suite.Suite
import org.codehaus.groovy.runtime.IOGroovyMethods



Suite.metaClass.show_cache_hotspot = { String computeGroup = null, String table = null ->
    //show cache hotspot "/<compute group>/<table>"
    def select_table_hotspot = """
        select
            partition_id as PartitionId,
            partition_name as PartitionName
        from __internal_schema.cloud_cache_hotspot
        where
            cluster_name = "${computeGroup}"  
            and table_name = "${table}" 
        group by
            cluster_id,
            cluster_name,
            table_id,
            table_name,
            partition_id,
            partition_name;
    """
    //show cache hotspot "/<compute group>"
    def select_compute_group_hotspot = """
        with t1 as (
            select
                cluster_id,
                cluster_name,
                table_id,
                table_name,
                insert_day,
                sum(query_per_day) as query_per_day_total,
                sum(query_per_week) as query_per_week_total
            from __internal_schema.cloud_cache_hotspot
            where cluster_name = "${computeGroup}" 
            group by cluster_id, cluster_name, table_id, table_name, insert_day
            )
            select
            cluster_id as ComputeGroupId,
            cluster_name as ComputeGroupName,
            table_id as TableId,
            table_name as TableName
            from (
            select
                row_number() over (
                partition by cluster_id
                order by insert_day desc, query_per_day_total desc, query_per_week_total desc
                ) as dr2,
                *
            from t1
            ) t2
            where dr2 = 1;
    """
    //show cache hotspot "/"
    def select_all_hotspot = """
        with t1 as (
            select
                cluster_id,
                cluster_name,
                table_id,
                table_name,
                insert_day,
                sum(query_per_day) as query_per_day_total,
                sum(query_per_week) as query_per_week_total
            from __internal_schema.cloud_cache_hotspot
            group by cluster_id, cluster_name, table_id, table_name, insert_day
            )
            select
            cluster_id as ComputeGroupId,
            cluster_name as ComputeGroupName,
            table_id as TableId,
            table_name as TableName
            from (
            select
                row_number() over (
                partition by cluster_id
                order by insert_day desc, query_per_day_total desc, query_per_week_total desc
                ) as dr2,
                *
            from t1
            ) t2
            where dr2 = 1;
    """
    def res = null
    if ( computeGroup != null  && table != null ){
        res = sql_return_maparray """${select_table_hotspot}"""
    }

    if ( computeGroup != null && table == null) {
        res = sql_return_maparray """${select_compute_group_hotspot}"""
    }

    if ( computeGroup == null && table == null) {
        res = sql_return_maparray """${select_all_hotspot}"""
    }
    return res

}
