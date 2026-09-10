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

package org.apache.doris.common;

import org.apache.doris.tso.TSOTimestamp;

/** A bounded incremental read must retry the same window after its upper bound becomes readable. */
public class IncrWindowNotReadyException extends UserException {
    private final long requestedEndTimestampMs;
    private final long committedTso;
    private final long retryAfterMs;

    public IncrWindowNotReadyException(long requestedEndTimestampMs, long committedTso, long retryAfterMs) {
        super(String.format("ERR_INCR_WINDOW_NOT_READY: requestedEndTimestampMs=%d, committedTSO=%d, "
                        + "committedTSOPhysicalTimeMs=%d, retryAfterMs=%d",
                requestedEndTimestampMs, committedTso, TSOTimestamp.extractPhysicalTime(committedTso), retryAfterMs));
        setMysqlErrorCode(ErrorCode.ERR_INCR_WINDOW_NOT_READY);
        this.requestedEndTimestampMs = requestedEndTimestampMs;
        this.committedTso = committedTso;
        this.retryAfterMs = retryAfterMs;
    }

    public long getRequestedEndTimestampMs() {
        return requestedEndTimestampMs;
    }

    public long getCommittedTso() {
        return committedTso;
    }

    public long getRetryAfterMs() {
        return retryAfterMs;
    }
}
