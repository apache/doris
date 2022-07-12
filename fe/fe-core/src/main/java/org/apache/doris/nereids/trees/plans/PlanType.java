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
    LOGICAL_UNBOUND_RELATION,
    LOGICAL_BOUND_RELATION,
    LOGICAL_PROJECT,
    LOGICAL_FILTER,
    LOGICAL_JOIN,
    LOGICAL_AGGREGATE,
    LOGICAL_SORT,
    LOGICAL_OLAP_SCAN,
    GROUP_PLAN,

    // physical plan
    PHYSICAL_OLAP_SCAN,
    PHYSICAL_PROJECT,
    PHYSICAL_FILTER,
    PHYSICAL_BROADCAST_HASH_JOIN,
    PHYSICAL_AGGREGATE,
    PHYSICAL_SORT,
    PHYSICAL_HASH_JOIN,
    PHYSICAL_EXCHANGE,
    PHYSICAL_DISTRIBUTION;
}
