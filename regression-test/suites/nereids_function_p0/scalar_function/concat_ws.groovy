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

suite("nereids_scalar_fn_concat_ws") {

    sql 'use regression_test_nereids_function_p0_scalar_function'
    qt_concat_ws_ArrayWithNullElement "select concat_ws('-',['a','b'],['css',null,'d'],['g','f'],['s'])"


    qt_concat_ws_ArrayWithEmptyString "select concat_ws('-',['a',''],['','css'],['d',''])"
    qt_concat_ws_WithEmptyArray "select concat_ws('-',['a','b'],[],['css','d'],[])"
    qt_concat_ws_SeparatorSpecial "select concat_ws('|',['x','y'],['m',null,'n'],['p'])"

    qt_concat_ws_SeparatorEmpty "select concat_ws('',['a','b'],['c',null],['d'])"

    qt_concat_ws_ArrayWithNumber "select concat_ws('-',['1','2'],['3',null,'4'],['5','6'])"
    qt_concat_ws_WithNullArray "select concat_ws('-',['a'],null,['b','c'])"
    qt_concat_ws_SingleArray "select concat_ws(',',['x','y','z'])"
    qt_concat_ws_ArrayAllNull "select concat_ws('-',['a'],[null,null],['b'])"
    qt_concat_ws_MixedTypeElement "select concat_ws('|',['a','123'],['456',null,'b'])"
    qt_concat_ws_chinese "select concat_ws('，',['你好','世界'],['Doris',null,'Nereids'],['测试'])"

    sql "DROP TABLE IF EXISTS test_concat_ws_1"
    sql "CREATE TABLE test_concat_ws_1 (id INT, a ARRAY<VARCHAR>, b ARRAY<VARCHAR>) ENGINE=OLAP DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')"
    sql "INSERT INTO test_concat_ws_1 VALUES (1, ['a','b'], ['css',null,'d']), (2, ['x',null], ['y','z']),(3,['你好','世界'],['Doris',null,'Nereids'])"
    qt_concat_ws_insert_1 "SELECT concat_ws('-', a, b) FROM test_concat_ws_1 ORDER BY id"

}