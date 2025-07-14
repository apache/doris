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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/PlannerContext.java
// and modified by Doris

package org.apache.doris.planner;

/**
 * Contains the analysis result of a query as well as planning-specific
 * parameters and state such as plan-node and plan-fragment id generators.
 */
public class PlannerContext {

    // Estimate of the overhead imposed by storing data in a hash tbl;
    // used for determining whether a broadcast join is feasible.
    public static final double HASH_TBL_SPACE_OVERHEAD = 1.1;
}
