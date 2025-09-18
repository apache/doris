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

suite('test_agg_nullable') {
    sql 'DROP TABLE IF EXISTS test_agg_nullable_t1 FORCE'
    sql "CREATE TABLE test_agg_nullable_t1(a int not null, b int not null, c int not null) distributed by hash(a) properties('replication_num' = '1')"
    sql "SET detail_shape_nodes='PhysicalProject'"
    order_qt_agg_nullable '''
        select k > 10 and k < 5 from  (select sum(a) as k from test_agg_nullable_t1) s
    '''
    qt_agg_nullable_shape '''explain shape plan
        select k > 10 and k < 5 from  (select sum(a) as k from test_agg_nullable_t1) s
    '''
    sql 'DROP TABLE IF EXISTS test_agg_nullable_t1 FORCE'
}

