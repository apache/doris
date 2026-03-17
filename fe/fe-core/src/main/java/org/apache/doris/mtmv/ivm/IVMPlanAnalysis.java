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

import com.google.common.base.Preconditions;

import java.util.Objects;
import java.util.Optional;

/** Result of IVM plan analysis. */
public final class IVMPlanAnalysis {
    private final boolean valid;
    private final Optional<IVMPlanPattern> pattern;
    private final Optional<String> invalidReason;

    private IVMPlanAnalysis(boolean valid, Optional<IVMPlanPattern> pattern, Optional<String> invalidReason) {
        this.valid = valid;
        this.pattern = pattern;
        this.invalidReason = invalidReason;
    }

    public static IVMPlanAnalysis of(IVMPlanPattern pattern) {
        return new IVMPlanAnalysis(true,
                Optional.of(Objects.requireNonNull(pattern, "pattern can not be null")),
                Optional.empty());
    }

    public static IVMPlanAnalysis unsupported(String unsupportedReason) {
        return new IVMPlanAnalysis(false, Optional.empty(),
                Optional.of(Objects.requireNonNull(unsupportedReason, "unsupportedReason can not be null")));
    }

    public boolean isValid() {
        return valid;
    }

    public boolean isInvalid() {
        return !valid;
    }

    public IVMPlanPattern getPattern() {
        Preconditions.checkArgument(valid, "pattern only exists on valid IVM plan analysis");
        return pattern.get();
    }

    public String getUnsupportedReason() {
        Preconditions.checkArgument(!valid, "unsupported reason only exists on invalid IVM plan analysis");
        return invalidReason.get();
    }
}
