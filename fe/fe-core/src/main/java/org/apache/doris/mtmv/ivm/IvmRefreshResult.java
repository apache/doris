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

import java.util.Objects;

/** Result of one FE-side incremental refresh attempt. */
public class IvmRefreshResult {
    private final boolean success;
    private final IvmFailureReason failureReason;
    private final String detailMessage;

    private IvmRefreshResult(boolean success, IvmFailureReason failureReason, String detailMessage) {
        this.success = success;
        this.failureReason = failureReason;
        this.detailMessage = detailMessage;
    }

    public static IvmRefreshResult success() {
        return new IvmRefreshResult(true, null, null);
    }

    public static IvmRefreshResult fallback(IvmFailureReason failureReason, String detailMessage) {
        return new IvmRefreshResult(false,
                Objects.requireNonNull(failureReason, "failureReason can not be null"),
                detailMessage == null ? failureReason.name() : detailMessage);
    }

    public boolean isSuccess() {
        return success;
    }

    public IvmFailureReason getFailureReason() {
        return failureReason;
    }

    public String getDetailMessage() {
        return detailMessage;
    }

    @Override
    public String toString() {
        if (success) {
            return "IvmRefreshResult{success=true}";
        }
        return "IvmRefreshResult{"
                + "success=false"
                + ", failureReason=" + failureReason
                + ", detailMessage='" + detailMessage + '\''
                + '}';
    }
}
