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

package org.apache.doris.nereids.trees.plans;

/**
 * Types for all Plan in Nereids.
 */
public enum PlanType {
    // special
    GROUP_PLAN,
    UNKNOWN,

    // logical plans
    // logical relations
    LOGICAL_BOUND_RELATION,
    LOGICAL_CTE_CONSUMER,
    LOGICAL_FILE_SCAN,
    LOGICAL_EMPTY_RELATION,
    LOGICAL_ES_SCAN,
    LOGICAL_JDBC_SCAN,
    LOGICAL_OLAP_SCAN,
    LOGICAL_ONE_ROW_RELATION,
    LOGICAL_SCHEMA_SCAN,
    LOGICAL_TVF_RELATION,
    LOGICAL_UNBOUND_ONE_ROW_RELATION,
    LOGICAL_UNBOUND_RELATION,
    LOGICAL_UNBOUND_TVF_RELATION,

    // logical sinks
    LOGICAL_FILE_SINK,
    LOGICAL_OLAP_TABLE_SINK,
    LOGICAL_RESULT_SINK,
    LOGICAL_UNBOUND_OLAP_TABLE_SINK,
    LOGICAL_UNBOUND_RESULT_SINK,

    // logical others
    LOGICAL_AGGREGATE,
    LOGICAL_APPLY,
    LOGICAL_ASSERT_NUM_ROWS,
    LOGICAL_CHECK_POLICY,
    LOGICAL_CTE,
    LOGICAL_CTE_ANCHOR,
    LOGICAL_CTE_PRODUCER,
    LOGICAL_EXCEPT,
    LOGICAL_FILTER,
    LOGICAL_GENERATE,
    LOGICAL_HAVING,
    LOGICAL_INTERSECT,
    LOGICAL_JOIN,
    LOGICAL_LIMIT,
    LOGICAL_MULTI_JOIN,
    LOGICAL_PARTITION_TOP_N,
    LOGICAL_PROJECT,
    LOGICAL_REPEAT,
    LOGICAL_SELECT_HINT,
    LOGICAL_SUBQUERY_ALIAS,
    LOGICAL_SORT,
    LOGICAL_TOP_N,
    LOGICAL_UNION,
    LOGICAL_USING_JOIN,
    LOGICAL_WINDOW,

    // physical plans
    // logical relations
    PHYSICAL_CTE_CONSUMER,
    PHYSICAL_EMPTY_RELATION,
    PHYSICAL_ES_SCAN,
    PHYSICAL_FILE_SCAN,
    PHYSICAL_JDBC_SCAN,
    PHYSICAL_ONE_ROW_RELATION,
    PHYSICAL_OLAP_SCAN,
    PHYSICAL_SCHEMA_SCAN,
    PHYSICAL_TVF_RELATION,

    // logical sinks
    PHYSICAL_FILE_SINK,
    PHYSICAL_OLAP_TABLE_SINK,
    PHYSICAL_RESULT_SINK,

    // logical others
    PHYSICAL_HASH_AGGREGATE,
    PHYSICAL_ASSERT_NUM_ROWS,
    PHYSICAL_CTE_PRODUCER,
    PHYSICAL_CTE_ANCHOR,
    PHYSICAL_DISTRIBUTE,
    PHYSICAL_EXCEPT,
    PHYSICAL_FILTER,
    PHYSICAL_GENERATE,
    PHYSICAL_INTERSECT,
    PHYSICAL_HASH_JOIN,
    PHYSICAL_NESTED_LOOP_JOIN,
    PHYSICAL_LIMIT,
    PHYSICAL_PARTITION_TOP_N,
    PHYSICAL_PROJECT,
    PHYSICAL_REPEAT,
    PHYSICAL_LOCAL_QUICK_SORT,
    PHYSICAL_QUICK_SORT,
    PHYSICAL_TOP_N,
    PHYSICAL_UNION,
    PHYSICAL_WINDOW,

    // commands
    CREATE_POLICY_COMMAND,
    CREATE_TABLE_COMMAND,
    DELETE_COMMAND,
    EXPLAIN_COMMAND,
    EXPORT_COMMAND,
    INSERT_INTO_TABLE_COMMAND,
    LOAD_COMMAND,
    SELECT_INTO_OUTFILE_COMMAND,
    UPDATE_COMMAND
}
