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

package org.apache.doris.nereids.types;

import org.apache.doris.common.GlobRegexUtil;
import org.apache.doris.thrift.TPatternType;

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

import java.util.Objects;

/**
 * A skip pattern inside a VariantType.
 * Specifies field paths that should be irreversibly pruned during data ingestion.
 */
public class VariantSkipPattern {
    private final String pattern;
    private final TPatternType patternType;

    /**
     * VariantSkipPattern Constructor with default MATCH_NAME_GLOB pattern type.
     */
    public VariantSkipPattern(String pattern) {
        this(pattern, TPatternType.MATCH_NAME_GLOB.name());
    }

    /**
     * VariantSkipPattern Constructor.
     * Validates glob patterns at DDL time — invalid globs are rejected immediately.
     *
     * @param pattern the glob or exact pattern string
     * @param patternType "MATCH_NAME" for exact match, otherwise MATCH_NAME_GLOB
     */
    public VariantSkipPattern(String pattern, String patternType) {
        this.pattern = Objects.requireNonNull(pattern, "pattern should not be null");
        TPatternType type;
        if (TPatternType.MATCH_NAME.name().equalsIgnoreCase(patternType)) {
            type = TPatternType.MATCH_NAME;
        } else {
            type = TPatternType.MATCH_NAME_GLOB;
        }
        this.patternType = type;
        // DDL-time validation: compile glob to catch syntax errors early
        if (this.patternType == TPatternType.MATCH_NAME_GLOB) {
            try {
                GlobRegexUtil.getOrCompilePattern(this.pattern);
            } catch (PatternSyntaxException | IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Invalid glob pattern for SKIP: '" + this.pattern + "': " + e.getMessage(), e);
            }
        }
    }

    public String getPattern() {
        return pattern;
    }

    public TPatternType getPatternType() {
        return patternType;
    }

    /**
     * Check if the given field path matches this skip pattern.
     * Note: This method is currently unused in FE. The actual skip pattern matching
     * is performed in BE's JSON parser (should_skip_path) during data ingestion.
     * Kept here for potential future FE-side validation or testing use.
     */
    public boolean matches(String fieldPath) {
        if (patternType == TPatternType.MATCH_NAME) {
            return pattern.equals(fieldPath);
        }
        try {
            Pattern compiled = GlobRegexUtil.getOrCompilePattern(pattern);
            return compiled.matcher(fieldPath).matches();
        } catch (PatternSyntaxException | IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Convert to SQL string representation.
     */
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SKIP ");
        if (patternType == TPatternType.MATCH_NAME) {
            sb.append("MATCH_NAME ");
        }
        sb.append("'").append(pattern).append("'");
        return sb.toString();
    }

    /**
     * Convert to Catalog layer type.
     */
    public org.apache.doris.catalog.VariantSkipPattern toCatalogType() {
        return new org.apache.doris.catalog.VariantSkipPattern(pattern, patternType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VariantSkipPattern that = (VariantSkipPattern) o;
        return Objects.equals(pattern, that.pattern) && patternType == that.patternType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern, patternType);
    }

    @Override
    public String toString() {
        return toSql();
    }
}
