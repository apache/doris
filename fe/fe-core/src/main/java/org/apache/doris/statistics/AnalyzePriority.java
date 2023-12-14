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

package org.apache.doris.statistics;

/**
 * Determine the analyzing priority when doing auto analyze.
 */
public enum AnalyzePriority {
    /**
     * For column which used in predicate.
     */
    HIGH(0),
    /**
     * For columns that get queried.
     */
    NORMAL(1),
    /**
     * Others.
     */
    LOW(2),
    /**
     * For manual jobs
     */
    NONE(3);

    public final int val;

    AnalyzePriority(int val) {
        this.val = val;
    }

}
