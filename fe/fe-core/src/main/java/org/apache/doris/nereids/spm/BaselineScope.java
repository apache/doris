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

package org.apache.doris.nereids.spm;

/**
 * BaselineScope - baseline storage scope enum (design doc 6.13.1).
 *
 * - GLOBAL: persisted baseline (Phase 2 writes it to the __internal_schema.spm_baselines
 *   internal table); survives FE restarts.
 * - SESSION: session-local baseline kept in FE memory; cleared when the session ends or
 *   the FE restarts. Used for temporary testing and validation.
 *
 * Syntax: CREATE [GLOBAL | SESSION] BASELINE PLAN 'bindSql' WITH 'planSql'.
 * GLOBAL is the default when neither keyword is given.
 *
 * <p>The scope of an existing baseline is uniquely determined by its id (see
 * {@link #ofId(long)}), because the two scopes allocate from disjoint id ranges:
 *
 * <ul>
 *   <li>GLOBAL: [1, {@link #SESSION_ID_BASE}) - the shared BaselineManager generator
 *       allocates from 1 and the persistence-layer watermark only ever pushes it
 *       further up.</li>
 *   <li>SESSION: [{@link #SESSION_ID_BASE}, 2^63) - each FE process allocates session
 *       ids from a counter that starts at 2^62.</li>
 * </ul>
 *
 * Therefore a session id can never collide with any GLOBAL id observed anywhere in the
 * cluster, while two FE processes may hand out the same numeric session ids - a session
 * baseline never leaves its FE/connection, so those values are never observed together.
 * Address resolution (SHOW / ALTER / DROP / EXPLAIN) must rely on ofId and must never
 * infer the scope from which store happens to contain the id.
 */
public enum BaselineScope {
    /** Global baseline (persisted; the default). */
    GLOBAL("GLOBAL"),
    /** Session-local baseline (in-memory only). */
    SESSION("SESSION");

    /**
     * First id of the SESSION id range (2^62). "The bottom half" [0, 2^62) is reserved
     * for the GLOBAL generator, so the two scopes can never hand out the same id; session
     * ids stay in [2^62, 2^63) and are therefore never negative.
     */
    public static final long SESSION_ID_BASE = 1L << 62;

    private final String name;

    BaselineScope(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Returns the scope a baseline id belongs to. This is the single authoritative
     * judgement; callers must never infer the scope from the store that happens to
     * contain the id (existence-based guessing routes a session-id lookup into the
     * shared store and a global-id lookup into the connection-local store).
     *
     * @param id the baseline id
     * @return SESSION when the id is in [2^62, 2^63), GLOBAL for [1, 2^62)
     */
    public static BaselineScope ofId(long id) {
        return id >= SESSION_ID_BASE ? SESSION : GLOBAL;
    }

    /**
     * Parses from a string (case-insensitive).
     *
     * @param name the scope name; null / unknown falls back to GLOBAL
     * @return the matching scope
     */
    public static BaselineScope fromString(String name) {
        if (name != null) {
            for (BaselineScope scope : values()) {
                if (scope.name.equalsIgnoreCase(name)) {
                    return scope;
                }
            }
        }
        return GLOBAL;
    }
}
