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

/** Result of checking whether incremental refresh is viable for a materialized view. */
public class IVMCapabilityResult {
    private final boolean incremental;
    private final FallbackReason fallbackReason;
    private final String detailMessage;

    private IVMCapabilityResult(boolean incremental, FallbackReason fallbackReason, String detailMessage) {
        this.incremental = incremental;
        this.fallbackReason = fallbackReason;
        this.detailMessage = detailMessage;
    }

    public static IVMCapabilityResult ok() {
        return new IVMCapabilityResult(true, null, null);
    }

    public static IVMCapabilityResult unsupported(FallbackReason fallbackReason, String detailMessage) {
        return new IVMCapabilityResult(false,
                Objects.requireNonNull(fallbackReason, "fallbackReason can not be null"),
                Objects.requireNonNull(detailMessage, "detailMessage can not be null"));
    }

    public boolean isIncremental() {
        return incremental;
    }

    public FallbackReason getFallbackReason() {
        return fallbackReason;
    }

    public String getDetailMessage() {
        return detailMessage;
    }

    @Override
    public String toString() {
        if (incremental) {
            return "IVMCapabilityResult{incremental=true}";
        }
        return "IVMCapabilityResult{"
                + "incremental=false"
                + ", fallbackReason=" + fallbackReason
                + ", detailMessage='" + detailMessage + '\''
                + '}';
    }
}
