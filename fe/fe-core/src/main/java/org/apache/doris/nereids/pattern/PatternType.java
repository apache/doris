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

package org.apache.doris.nereids.pattern;

/**
 * Types for all Pattern type.
 * <p>
 * 1. NORMAL:      normal pattern matching, e.g. match by planId or class type.
 * 2. ANY:         match any plan, return a plan when matched.
 * 3. MULTI:       match multiple children plans, that we don't know how many children exist.
 *                 only use as the last child pattern, and can not use as the top pattern.
 *                 return some children plan with real plan type when matched.
 * 4. GROUP:       match a group plan, only use in a pattern's children, and can not use as the top pattern.
 *                 return a GroupPlan when matched.
 * 5. MULTI_GROUP: match multiple group plan, that we don't know how many children group exist.
 *                 only use in a pattern's children, so can not use as the top pattern.
 *                 return some children GroupPlan when matched.
 * </p>
 */
public enum PatternType {
    NORMAL,
    ANY,
    MULTI,
    GROUP,
    MULTI_GROUP
}
