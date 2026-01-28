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
 * A field inside a VariantType.
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
     * VariantField Constructor
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

    public TPatternType getPatternType() {
        return patternType;
    }

    /**
     * Check if the given field name matches this field's pattern.
     * This method aligns with BE's fnmatch(pattern, path, FNM_PATHNAME) behavior.
     *
     * Supported glob syntax:
     * - '*' matches any sequence of characters except '/'
     * - '?' matches any single character except '/'
     * - '[...]' matches any character in the brackets
     * - '[!...]' or '[^...]' matches any character not in the brackets
     *
     * @param fieldName the field name to check
     * @return true if the field name matches the pattern
     */
    public boolean matches(String fieldName) {
        if (patternType == TPatternType.MATCH_NAME) {
            return pattern.equals(fieldName);
        } else {
            // MATCH_NAME_GLOB: convert glob pattern to regex
            // This aligns with BE's fnmatch(pattern, path, FNM_PATHNAME)
            String regex = globToRegex(pattern);
            return fieldName.matches(regex);
        }
    }

    /**
     * Convert glob pattern to regex pattern, aligning with fnmatch(FNM_PATHNAME) behavior.
     */
    private static String globToRegex(String glob) {
        StringBuilder regex = new StringBuilder();
        int i = 0;
        int len = glob.length();

        while (i < len) {
            char c = glob.charAt(i);
            switch (c) {
                case '*':
                    // '*' matches any sequence of characters except '/' (FNM_PATHNAME)
                    regex.append("[^/]*");
                    break;
                case '?':
                    // '?' matches any single character except '/' (FNM_PATHNAME)
                    regex.append("[^/]");
                    break;
                case '[':
                    // Character class - find the closing bracket
                    int j = i + 1;
                    // Handle negation: [! or [^
                    if (j < len && (glob.charAt(j) == '!' || glob.charAt(j) == '^')) {
                        j++;
                    }
                    // Handle ] as first character in class
                    if (j < len && glob.charAt(j) == ']') {
                        j++;
                    }
                    // Find closing ]
                    while (j < len && glob.charAt(j) != ']') {
                        j++;
                    }
                    if (j >= len) {
                        // No closing bracket, treat [ as literal
                        regex.append("\\[");
                    } else {
                        // Extract the character class content
                        String classContent = glob.substring(i + 1, j);
                        regex.append('[');
                        // Convert [! to [^
                        if (classContent.startsWith("!")) {
                            regex.append('^').append(classContent.substring(1));
                        } else {
                            regex.append(classContent);
                        }
                        regex.append(']');
                        i = j; // Move past the closing ]
                    }
                    break;
                // Escape regex special characters
                case '\\':
                case '.':
                case '(':
                case ')':
                case '{':
                case '}':
                case '+':
                case '^':
                case '$':
                case '|':
                    regex.append('\\').append(c);
                    break;
                default:
                    regex.append(c);
                    break;
            }
            i++;
        }
        return regex.toString();
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
