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
public class IVMRefreshResult {
    private final boolean success;
    private final FallbackReason fallbackReason;
    private final String detailMessage;

    private IVMRefreshResult(boolean success, FallbackReason fallbackReason, String detailMessage) {
        this.success = success;
        this.fallbackReason = fallbackReason;
        this.detailMessage = detailMessage;
    }

    public static IVMRefreshResult success() {
        return new IVMRefreshResult(true, null, null);
    }

    public static IVMRefreshResult fallback(FallbackReason fallbackReason, String detailMessage) {
        return new IVMRefreshResult(false,
                Objects.requireNonNull(fallbackReason, "fallbackReason can not be null"),
                Objects.requireNonNull(detailMessage, "detailMessage can not be null"));
    }

    public boolean isSuccess() {
        return success;
    }

    public FallbackReason getFallbackReason() {
        return fallbackReason;
    }

    public String getDetailMessage() {
        return detailMessage;
    }

    @Override
    public String toString() {
        if (success) {
            return "IVMRefreshResult{success=true}";
        }
        return "IVMRefreshResult{"
                + "success=false"
                + ", fallbackReason=" + fallbackReason
                + ", detailMessage='" + detailMessage + '\''
                + '}';
    }
}
