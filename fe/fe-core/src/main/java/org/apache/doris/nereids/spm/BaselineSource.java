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
 * BaselineSource - baseline source enum.
 *
 * Corresponds to design doc section 6.1: distinguishes manually created baselines
 * (USER) from auto-captured ones (CAPTURE, Phase 2), used for SHOW filtering, audit
 * tracing and candidate baseline ordering.
 */
public enum BaselineSource {
    /** Manually created by the user via CREATE BASELINE PLAN. */
    USER("USER"),
    /** Auto-captured (Phase 2, created by scanning audit_log). */
    CAPTURE("CAPTURE");

    private final String name;

    BaselineSource(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Parses from a string.
     *
     * @param name the source name (case-insensitive)
     * @return the matching enum, or USER by default when unknown
     */
    public static BaselineSource fromString(String name) {
        if (name != null) {
            for (BaselineSource source : values()) {
                if (source.name.equalsIgnoreCase(name)) {
                    return source;
                }
            }
        }
        return USER;
    }
}
