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
    UNKNOWN,

    // logical plan
    LOGICAL_OLAP_TABLE_SINK,
    LOGICAL_CTE,
    LOGICAL_WINDOW,
    LOGICAL_SUBQUERY_ALIAS,
    LOGICAL_UNBOUND_ONE_ROW_RELATION,
    LOGICAL_EMPTY_RELATION,
    LOGICAL_ONE_ROW_RELATION,
    LOGICAL_UNBOUND_RELATION,
    LOGICAL_UNBOUND_TVF_RELATION,
    LOGICAL_BOUND_RELATION,
    LOGICAL_UNBOUND_OLAP_TABLE_SINK,
    LOGICAL_TVF_RELATION,
    LOGICAL_PROJECT,
    LOGICAL_FILTER,
    LOGICAL_GENERATE,
    LOGICAL_JOIN,
    LOGICAL_AGGREGATE,
    LOGICAL_REPEAT,
    LOGICAL_SORT,
    LOGICAL_TOP_N,
    LOGICAL_PARTITION_TOP_N,
    LOGICAL_LIMIT,
    LOGICAL_OLAP_SCAN,
    LOGICAL_SCHEMA_SCAN,
    LOGICAL_FILE_SCAN,
    LOGICAL_JDBC_SCAN,
    LOGICAL_ES_SCAN,
    LOGICAL_APPLY,
    LOGICAL_SELECT_HINT,
    LOGICAL_ASSERT_NUM_ROWS,
    LOGICAL_HAVING,
    LOGICAL_MULTI_JOIN,
    LOGICAL_CHECK_POLICY,
    LOGICAL_UNION,
    LOGICAL_EXCEPT,
    LOGICAL_INTERSECT,
    LOGICAL_USING_JOIN,
    LOGICAL_CTE_RELATION,
    LOGICAL_CTE_ANCHOR,
    LOGICAL_CTE_PRODUCER,
    LOGICAL_CTE_CONSUMER,
    LOGICAL_FILE_SINK,

    GROUP_PLAN,

    // physical plan
    PHYSICAL_OLAP_TABLE_SINK,
    PHYSICAL_CTE_PRODUCE,
    PHYSICAL_CTE_CONSUME,
    PHYSICAL_CTE_ANCHOR,
    PHYSICAL_WINDOW,
    PHYSICAL_EMPTY_RELATION,
    PHYSICAL_ONE_ROW_RELATION,
    PHYSICAL_OLAP_SCAN,
    PHYSICAL_FILE_SCAN,
    PHYSICAL_JDBC_SCAN,
    PHYSICAL_ES_SCAN,
    PHYSICAL_TVF_RELATION,
    PHYSICAL_SCHEMA_SCAN,
    PHYSICAL_PROJECT,
    PHYSICAL_FILTER,
    PHYSICAL_GENERATE,
    PHYSICAL_BROADCAST_HASH_JOIN,
    PHYSICAL_AGGREGATE,
    PHYSICAL_REPEAT,
    PHYSICAL_QUICK_SORT,
    PHYSICAL_TOP_N,
    PHYSICAL_PARTITION_TOP_N,
    PHYSICAL_LOCAL_QUICK_SORT,
    PHYSICAL_LIMIT,
    PHYSICAL_HASH_JOIN,
    PHYSICAL_NESTED_LOOP_JOIN,
    PHYSICAL_EXCHANGE,
    PHYSICAL_DISTRIBUTION,
    PHYSICAL_ASSERT_NUM_ROWS,
    PHYSICAL_UNION,
    PHYSICAL_EXCEPT,
    PHYSICAL_INTERSECT,
    PHYSICAL_FILE_SINK,

    COMMAND,
    EXPLAIN_COMMAND,
    CREATE_POLICY_COMMAND,
    INSERT_INTO_TABLE_COMMAND,
    UPDATE_COMMAND,
    DELETE_COMMAND,
    SELECT_INTO_OUTFILE_COMMAND
}
