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

import org.apache.doris.nereids.util.Utils;
import org.apache.doris.thrift.TPatternType;

import java.util.Objects;

/**
 * A field inside a StructType.
 */
public class VariantField {
    private final String pattern;
    private final DataType dataType;
    private final String comment;
    private final TPatternType patternType;

    public VariantField(String pattern, DataType dataType, String comment) {
        this(pattern, dataType, comment, TPatternType.MATCH_NAME_GLOB.name());
    }

    /**
     * StructField Constructor
     *  @param pattern of this field
     *  @param dataType The data type of this field
     *  @param comment The comment of this field
     *  @param patternType The patternType of this field
     */
    public VariantField(String pattern, DataType dataType, String comment, String patternType) {
        this.pattern = Objects.requireNonNull(pattern, "pattern should not be null");
        this.dataType = Objects.requireNonNull(dataType, "dataType should not be null");
        this.comment = Objects.requireNonNull(comment, "comment should not be null");
        TPatternType type;
        if (TPatternType.MATCH_NAME.name().equalsIgnoreCase(patternType)) {
            type = TPatternType.MATCH_NAME;
        } else {
            type = TPatternType.MATCH_NAME_GLOB;
        }
        this.patternType = Objects.requireNonNull(type, "patternType should not be null");
    }

    public String getPattern() {
        return pattern;
    }

    public DataType getDataType() {
        return dataType;
    }

    public String getComment() {
        return comment;
    }

    public org.apache.doris.catalog.VariantField toCatalogDataType() {
        return new org.apache.doris.catalog.VariantField(
                pattern, dataType.toCatalogDataType(), comment, patternType);
    }

    /**
     * Convert this VariantField to SQL string representation.
     * @return SQL string representation of this VariantField
     */
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        if (patternType == TPatternType.MATCH_NAME) {
            sb.append(patternType.toString()).append(" ");
        }

        sb.append("'").append(pattern).append("'");
        sb.append(":").append(dataType.toSql());
        if (!comment.isEmpty()) {
            sb.append(" COMMENT '").append(comment).append("'");
        }
        return sb.toString();
    }

    public VariantField conversion() {
        return new VariantField(pattern, dataType.conversion(), comment, patternType.toString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VariantField that = (VariantField) o;
        return Objects.equals(pattern, that.pattern) && Objects.equals(dataType,
                that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern, dataType);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("VariantField",
                "pattern", pattern,
                "dataType", dataType,
                "comment", comment);
    }
}
