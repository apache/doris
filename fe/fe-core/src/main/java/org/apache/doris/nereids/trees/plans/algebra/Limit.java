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

package org.apache.doris.nereids.trees.plans.algebra;

/**
 * Common interface for logical/physical limit.
 */
public interface Limit {
    long getLimit();

    long getOffset();

    /**
     * This limit node has valid offset info or not.
     * We treat the limit as having a valid offset info only when `getOffset` result is a positive value.
     */
    default boolean hasValidOffset() {
        return getOffset() > 0;
    }
}
