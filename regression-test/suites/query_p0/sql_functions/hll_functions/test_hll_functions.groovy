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

suite("test_hll_functions") {
    sql """drop TABLE if EXISTS test_hll_func;"""
    sql """
            create table test_hll_func(
                dt date,
                id int,
                name char(10),
                province char(10),
                os char(10),
                pv hll hll_union
            )
            Aggregate KEY (dt,id,name,province,os)
            distributed by hash(id) buckets 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ 
            insert into test_hll_func
            SELECT
            	dt,id,name,province,os,pv
            from (
	            SELECT	'2022-05-05' as dt,'10001' as id,'test01' as name,'beijing' as province,'windows' as os,hll_hash('windows') as pv
	            union all
	            SELECT	'2022-05-05' as dt,'10002' as id,'test01' as name,'beijing' as province,'linux' as os,hll_hash('linux') as pv
	            union all                                         
	            SELECT	'2022-05-05' as dt,'10003' as id,'test01' as name,'beijing' as province,'macos' as os,hll_hash('macos') as pv
	            union all                                         
	            SELECT	'2022-05-05' as dt,'10004' as id,'test01' as name,'hebei' as province,'windows' as os,hll_hash('windows') as pv
	            union all                                         
	            SELECT	'2022-05-06' as dt,'10001' as id,'test01' as name,'shanghai' as province,'windows' as os,hll_hash('windows') as pv
	            union all                                         
	            SELECT	'2022-05-06' as dt,'10002' as id,'test01' as name,'shanghai' as province,'linux' as os,hll_hash('linux') as pv
	            union all                                         
	            SELECT	'2022-05-06' as dt,'10003' as id,'test01' as name,'jiangsu' as province,'macos' as os,hll_hash('macos') as pv
	            union all                                         
	            SELECT	'2022-05-06' as dt,'10004' as id,'test01' as name,'shanxi' as province,'windows' as os,hll_hash('windows') as pv
	            union all                                         
	            SELECT	'2022-05-07' as dt,'10005' as id,'test01' as name,'shanxi' as province,'windows' as os,hll_empty() as pv
            ) as a
        """

    qt_table_select "select hll_union_agg(hll_from_base64(hll_to_base64(pv))) from test_hll_func;"
    qt_table_select "select province, hll_union_agg(hll_from_base64(hll_to_base64(pv))) from test_hll_func group by province order by province;"
    qt_table_select "select hll_cardinality(hll_from_base64(hll_to_base64(pv))) as res from test_hll_func  order by res limit 1;"

    qt_const_select "select hll_cardinality(hll_from_base64(hll_to_base64(hll_hash('abc'))));"
    qt_const_select "select hll_cardinality(hll_from_base64(hll_to_base64(hll_hash(''))));"
    qt_const_select "select hll_cardinality(hll_from_base64(hll_to_base64(hll_hash(NULL))));"
    qt_const_select "select hll_to_base64(NULL);"
    qt_const_select "select hll_to_base64(hll_empty());"
    qt_const_select "select hll_to_base64(hll_hash('abc'));"
    qt_const_select "select hll_to_base64(hll_hash(''));"
}
