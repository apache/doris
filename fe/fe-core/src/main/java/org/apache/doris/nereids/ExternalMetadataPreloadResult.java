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

package org.apache.doris.nereids;

/** Summarizes whether external metadata preload ran and what it processed. */
class ExternalMetadataPreloadResult {
    private final boolean executed;
    private final int candidateTableCount;
    private final int preloadedTableCount;
    private final String skipReason;

    private ExternalMetadataPreloadResult(boolean executed, int candidateTableCount,
            int preloadedTableCount, String skipReason) {
        this.executed = executed;
        this.candidateTableCount = candidateTableCount;
        this.preloadedTableCount = preloadedTableCount;
        this.skipReason = skipReason;
    }

    static ExternalMetadataPreloadResult executed(int candidateTableCount, int preloadedTableCount) {
        return new ExternalMetadataPreloadResult(true, candidateTableCount, preloadedTableCount, "");
    }

    static ExternalMetadataPreloadResult skipped(int candidateTableCount, String skipReason) {
        return new ExternalMetadataPreloadResult(false, candidateTableCount, 0, skipReason);
    }

    boolean isExecuted() {
        return executed;
    }

    int getCandidateTableCount() {
        return candidateTableCount;
    }

    int getPreloadedTableCount() {
        return preloadedTableCount;
    }

    String getSkipReason() {
        return skipReason;
    }
}
