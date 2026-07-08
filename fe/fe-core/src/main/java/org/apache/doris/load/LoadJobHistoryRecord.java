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

package org.apache.doris.load;

import org.apache.doris.thrift.TLoadJob;

/**
 * A final-state import task snapshot to be persisted into the internal loads_history table.
 * Carries the unified 20-field row (exactly what information_schema.loads shows) plus the
 * structured identity fields the sync needs: a stable dedup key and the typed finish time
 * used for the UNIQUE KEY and the day-level partition column.
 */
public class LoadJobHistoryRecord {
    private final String recordKey;
    private final long finishTimeMs;
    private final TLoadJob row;

    public LoadJobHistoryRecord(String recordKey, long finishTimeMs, TLoadJob row) {
        this.recordKey = recordKey;
        this.finishTimeMs = finishTimeMs;
        this.row = row;
    }

    public String getRecordKey() {
        return recordKey;
    }

    public long getFinishTimeMs() {
        return finishTimeMs;
    }

    public TLoadJob getRow() {
        return row;
    }
}
