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

    test {
        sql """SELECT hll_from_base64(CAST('AgAAAAAAAABwAAAAAAAAAA==' AS VARCHAR(100))) AS result;"""
        exception "[RUNTIME_ERROR]hll_from_base64 decode failed";
    }

    // Cover sparse/full HLL hll_to_base64: max_serialized_size is much larger
    // than the real serialized size, which previously left ColumnString chars
    // oversized vs offsets and tripped sanity_check.
    qt_sparse_roundtrip "select hll_cardinality(hll_from_base64(hll_to_base64(hll_union(hll_hash(number))))) > 400 from numbers(\"number\"=\"500\");"
    qt_sparse_len_positive "select length(hll_to_base64(hll_union(hll_hash(number)))) > 0 from numbers(\"number\"=\"500\");"

    // hll_to_binary / hll_from_binary: byte-level counterparts of hll_to_base64 /
    // hll_from_base64. The binary form is what an ORC/Parquet hive BINARY column
    // (mapped to VARBINARY via 'enable.mapping.varbinary') holds.
    qt_hll_binary_table_roundtrip "select hll_union_agg(hll_from_binary(hll_to_binary(pv))) from test_hll_func;"
    qt_hll_binary_table_roundtrip2 "select province, hll_union_agg(hll_from_binary(hll_to_binary(pv))) from test_hll_func group by province order by province;"
    qt_hll_binary_table_card "select hll_cardinality(hll_from_binary(hll_to_binary(pv))) as res from test_hll_func order by res limit 1;"

    qt_hll_binary_const_roundtrip "select hll_cardinality(hll_from_binary(hll_to_binary(hll_hash('abc'))));"
    qt_hll_binary_const_empty "select hll_cardinality(hll_from_binary(hll_to_binary(hll_empty())));"
    qt_hll_binary_const_null "select hll_cardinality(hll_from_binary(hll_to_binary(hll_hash(NULL))));"
    qt_hll_binary_to_null "select hll_to_binary(NULL);"

    // invalid / truncated bytes yield row-level NULL instead of an error, unlike
    // hll_from_base64 which fails the query. Assert `is null` directly: hll_cardinality
    // maps a NULL argument to 0, so wrapping the call would also pass if the function
    // wrongly returned a valid empty HLL instead of NULL.
    qt_hll_binary_invalid_empty "select hll_from_binary(to_binary('')) is null;"
    qt_hll_binary_invalid_type "select hll_from_binary(to_binary('7f')) is null;"
    qt_hll_binary_invalid_truncated "select hll_from_binary(sub_binary(hll_to_binary(hll_hash('abc')), 1, 2)) is null;"
    // positive control: a valid payload must not be NULL, otherwise the three cases
    // above would pass for the wrong reason
    qt_hll_binary_valid_not_null "select hll_from_binary(hll_to_binary(hll_hash('abc'))) is null;"

    // mixed base64/binary byte level consistency
    qt_hll_binary_equals_base64 "select hll_cardinality(hll_from_binary(hll_to_binary(pv))) = hll_cardinality(hll_from_base64(hll_to_base64(pv))) from test_hll_func order by 1;"
}
