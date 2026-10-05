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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.
suite("inPredicate") {
    sql """
    set debug_skip_fold_constant=true;

    create table t
    (
        id int,
        name string
    ) 
    duplicate key (id) 
    distributed by hash(id) buckets 3 
    properties("replication_num" = "1");

    insert into t values (7, 'bbb');
    """

    qt_in_predicate_with_column_and_const """
    select  id as x1,       
        id in (6, 2 + 0.3) as x2,   
        id in (6, 2 + 0.3) as x3
    from t;
    """
}
