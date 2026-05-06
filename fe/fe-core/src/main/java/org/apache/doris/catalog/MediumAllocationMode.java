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

package org.apache.doris.catalog;

import org.apache.doris.common.AnalysisException;

import com.google.common.base.Strings;

/**
 * Defines how Doris decides the storage medium of a data property
 * (partition-level / table-level).
 *
 * <p>Hard-binding semantics with CREATE TABLE:
 * <ul>
 *   <li>{@code PROPERTIES("storage_medium"="ssd|hdd")} -> {@link #STRICT}</li>
 *   <li>no {@code storage_medium} property -> {@link #ADAPTIVE}</li>
 * </ul>
 *
 * <p>The mode drives replica placement:
 * <ul>
 *   <li>{@link #STRICT}: user explicitly requested a medium; placement must
 *       honour it and fail if the requested medium cannot be satisfied.</li>
 *   <li>{@link #ADAPTIVE}: medium is a hint; placement may pick any available
 *       medium according to cluster capacity.</li>
 * </ul>
 */
public enum MediumAllocationMode {
    STRICT("strict"),
    ADAPTIVE("adaptive");

    private final String value;

    MediumAllocationMode(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public boolean isStrict() {
        return this == STRICT;
    }

    public boolean isAdaptive() {
        return this == ADAPTIVE;
    }

    /**
     * Parse a user-provided string (case-insensitive, trimmed) into the enum.
     *
     * @throws AnalysisException if the value is null, blank or unrecognised.
     */
    public static MediumAllocationMode fromString(String value) throws AnalysisException {
        String trimmed = Strings.nullToEmpty(value).trim();
        if (trimmed.isEmpty()) {
            throw new AnalysisException("medium_allocation_mode cannot be null or empty");
        }
        for (MediumAllocationMode mode : values()) {
            if (mode.value.equalsIgnoreCase(trimmed)) {
                return mode;
            }
        }
        throw new AnalysisException(String.format(
                "Invalid medium_allocation_mode value: '%s'. Valid options are: 'strict', 'adaptive'",
                value));
    }
}
