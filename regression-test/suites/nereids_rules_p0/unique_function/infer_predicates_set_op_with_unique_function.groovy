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

suite('infer_predicates_set_op_with_unique_function') {
    sql 'SET enable_nereids_planner=true'
    sql 'SET runtime_filter_mode=OFF'
    sql 'SET enable_fallback_to_original_planner=false'
    sql "SET ignore_shape_nodes='PhysicalDistribute'"
    sql "SET detail_shape_nodes='PhysicalProject'"
    sql 'SET disable_nereids_rules=PRUNE_EMPTY_PARTITION'

    // InferPredicates must NOT clone a predicate containing rand()/uuid()
    // from the left branch of EXCEPT/INTERSECT onto the right branch through
    // slot substitution, because it would re-evaluate the unique function on
    // a different set of rows and change semantics.

    qt_except_with_rand '''
        explain shape plan
        (select id from t1 where t1.id + random() > 5)
        except
        (select id from t2)
        '''

    qt_intersect_with_rand '''
        explain shape plan
        (select id from t1 where t1.id + random() > 5)
        intersect
        (select id from t2)
        '''

    qt_except_with_uuid '''
        explain shape plan
        (select id from t1 where uuid_to_int(uuid()) + t1.id > 5)
        except
        (select id from t2)
        '''

    qt_intersect_with_uuid '''
        explain shape plan
        (select id from t1 where uuid_to_int(uuid()) + t1.id > 5)
        intersect
        (select id from t2)
        '''

    // Deterministic predicates must still be inferred across set-op branches.
    qt_except_deterministic '''
        explain shape plan
        (select id from t1 where t1.id + 1 > 5)
        except
        (select id from t2)
        '''
}
