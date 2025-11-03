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

package org.apache.doris.nereids.rules;

/**
 * Promise of rule, The value with a large promise has a higher priority.
 * NOTICE: we must ensure that the promise of the IMPLEMENT is greater than the promise of the others.
 */
public enum RulePromise {
    ANALYSIS,
    REWRITE,
    EXPLORE,
    IMPLEMENT,

    // just for check plan in UT
    PLAN_CHECK
    ;

    public int promise() {
        return ordinal();
    }
}
