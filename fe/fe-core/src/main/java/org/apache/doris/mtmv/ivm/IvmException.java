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

import org.apache.doris.nereids.exceptions.AnalysisException;

import java.util.Objects;

/** Exception for known IVM failures that can be mapped to a failure reason. */
public class IvmException extends AnalysisException {
    private final IvmFailureReason failureReason;

    public IvmException(IvmFailureReason failureReason, String message) {
        super(message);
        this.failureReason = Objects.requireNonNull(failureReason, "failureReason can not be null");
    }

    public IvmException(IvmFailureReason failureReason, String message, Throwable cause) {
        super(message, cause);
        this.failureReason = Objects.requireNonNull(failureReason, "failureReason can not be null");
    }

    public IvmFailureReason getFailureReason() {
        return failureReason;
    }
}
