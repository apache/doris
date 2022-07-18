// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.pattern;

/**
 * Types for all Pattern type.
 */
public enum PatternType {
    // Normal pattern matching, e.g. match by planId or class type.
    NORMAL,
    // Match any plan, return a plan when matched.
    ANY,
    // Match multiple children plans, that we don't know how many children exist.
    // Only use as the last child pattern, and can not use as the top pattern.
    MULTI,
    // Match a group plan, only use in a pattern's children, and can not use as the top pattern.
    // Return a GroupPlan when matched.
    GROUP,
    // Match multiple group plan, that we don't know how many children group exist.
    // Only use in a pattern's children, so can not use as the top pattern.
    // Return some children GroupPlan when matched.
    MULTI_GROUP,
    // Match a subtree of plan, plan nodes in the matched result are the subset of specified plan types when
    // declare the pattern.
    SUB_TREE,
}
