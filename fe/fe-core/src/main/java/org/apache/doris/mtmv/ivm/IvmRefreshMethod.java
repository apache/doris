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

package org.apache.doris.mtmv.ivm;

import java.util.Set;

/**
 * How a refresh task of an IVM MV actually refreshed it, judged by the kinds of work that committed data.
 *
 * <p>The request does not say it: an AUTO or INCREMENTAL FALLBACK request may end in the incremental refresh, a
 * partition rebuild, a whole-MV rebuild, or several of them, and which of them wrote data is what tells a refresh
 * that kept the MV up incrementally from one that recomputed it. The refresh mode a task reports is a different
 * question -- which partitions it covers -- so the two are reported side by side.
 */
public enum IvmRefreshMethod {
    /** Nothing was committed: every partition was up to date, or the task ended before writing anything. */
    NONE,
    /** Only the incremental refresh committed data. */
    INCREMENTAL,
    /**
     * Only partition rebuilds committed data. The rebuild an incremental attempt runs first, for the partitions the
     * criterion says cannot be caught up, is one of them.
     */
    PARTITION_REBUILD,
    /** Only a whole-MV rebuild committed data. */
    COMPLETE,
    /** More than one kind committed data, typically the rebuild of invalidated partitions and then the delta. */
    MIXED;

    /** The method of a task whose committed work was of these kinds, which are any of the three single kinds. */
    public static IvmRefreshMethod of(Set<IvmRefreshMethod> committedKinds) {
        if (committedKinds.isEmpty()) {
            return NONE;
        }
        if (committedKinds.size() > 1) {
            return MIXED;
        }
        return committedKinds.iterator().next();
    }
}
