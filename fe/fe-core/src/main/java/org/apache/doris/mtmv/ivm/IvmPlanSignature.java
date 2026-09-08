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

/**
 * IVM layout signature produced from the normalized maintenance plan.
 *
 * <p>The canonical string records only normalized-plan properties that may change
 * IVM row-id or hidden-column layout, such as hidden output expressions, scan
 * identity, join child order, and union arm order. The persisted signature stores
 * only the SHA-256 digest; the canonical string is kept in memory for diagnostics
 * and tests.
 *
 * <p>Example:
 * <pre>
 * canonicalString =
 *   IVM_LAYOUT_SIGNATURE_V1
 *   ROOT[hiddenOutput=[SLOT[name=__DORIS_IVM_ROW_ID_COL__]],
 *        plan=PROJECT[hiddenOutputs=[...],child=SCAN[table=ctl.db.t]]]
 * sha256 = 5a3f...e92c
 * </pre>
 */
public class IvmPlanSignature {
    /** Human-readable canonical representation used to compute {@link #sha256}. */
    private final String canonicalString;

    /** SHA-256 digest of {@link #canonicalString}; this is the compact value persisted on the MV. */
    private final String sha256;

    public IvmPlanSignature(String canonicalString, String sha256) {
        this.canonicalString = Objects.requireNonNull(canonicalString, "canonicalString can not be null");
        this.sha256 = Objects.requireNonNull(sha256, "sha256 can not be null");
    }

    public String getCanonicalString() {
        return canonicalString;
    }

    public String getSha256() {
        return sha256;
    }
}
