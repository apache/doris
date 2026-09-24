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
 * BaselineStatus - baseline status enum.
 *
 * Phase 1 uses the simple binary status ENABLED / DISABLED. Phase 3 will extend it to
 * ACCEPTED / FIXED / VERIFYING / REJECTED (the evolution model).
 */
public enum BaselineStatus {
    /** Participates in query rewrite matching. */
    ENABLED("ENABLED", true),
    /** Does not participate in matching (manually disabled). */
    DISABLED("DISABLED", false);

    private final String name;
    private final boolean active;

    BaselineStatus(String name, boolean active) {
        this.name = name;
        this.active = active;
    }

    /** Whether it participates in query rewrite matching. */
    public boolean isActive() {
        return active;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Parses from a string.
     *
     * @param name the status name (case-insensitive)
     * @return the matching enum, or ENABLED by default when unknown
     */
    public static BaselineStatus fromString(String name) {
        if (name != null) {
            for (BaselineStatus status : values()) {
                if (status.name.equalsIgnoreCase(name)) {
                    return status;
                }
            }
        }
        return ENABLED;
    }
}
