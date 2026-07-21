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
import java.util.Optional;

/**
 * Maps runtime guard failures back to stable IVM failure reasons.
 */
public final class IvmFailureClassifier {
    public static final String MIN_MAX_BOUNDARY_MSG_PREFIX = "IVM fallback: min/max boundary hit";
    public static final String BITMAP_AGG_DELETE_MSG_PREFIX =
            "IVM: deleted row affects BITMAP aggregate, requires COMPLETE";
    public static final String NON_DETERMINISTIC_ROW_ID_MSG_PREFIX =
            "IVM fallback: delete on non-deterministic row_id";

    private IvmFailureClassifier() {
    }

    public static Optional<IvmFailureReason> classifyExecutionFailure(String detail) {
        Objects.requireNonNull(detail, "detail can not be null");
        if (detail.contains(MIN_MAX_BOUNDARY_MSG_PREFIX)) {
            return Optional.of(IvmFailureReason.MIN_MAX_BOUNDARY_HIT);
        }
        if (detail.contains(BITMAP_AGG_DELETE_MSG_PREFIX)) {
            return Optional.of(IvmFailureReason.BITMAP_AGG_DELETE);
        }
        if (detail.contains(NON_DETERMINISTIC_ROW_ID_MSG_PREFIX)) {
            return Optional.of(IvmFailureReason.NON_DETERMINISTIC_ROW_ID);
        }
        return Optional.empty();
    }
}
