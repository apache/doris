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

import org.apache.doris.thrift.TPatternType;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

/**
 * Catalog-layer representation of a variant SKIP pattern.
 * Used for Gson persistence in FE metadata.
 */
public class VariantSkipPattern {

    @SerializedName(value = "p")
    private final String pattern;

    @SerializedName(value = "pt")
    private final TPatternType patternType;

    public VariantSkipPattern(String pattern, TPatternType patternType) {
        this.pattern = Objects.requireNonNull(pattern, "pattern should not be null");
        this.patternType = Objects.requireNonNull(patternType, "patternType should not be null");
    }

    public String getPattern() {
        return pattern;
    }

    public TPatternType getPatternType() {
        return patternType;
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SKIP ");
        if (patternType == TPatternType.MATCH_NAME) {
            sb.append("MATCH_NAME ");
        }
        sb.append("'").append(pattern).append("'");
        return sb.toString();
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
