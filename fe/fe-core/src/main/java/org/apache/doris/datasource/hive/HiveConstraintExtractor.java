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

package org.apache.doris.datasource.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Extracts primary key and unique key constraints from Hive table properties (TBLPROPERTIES).
 *
 * <h3>Convention:</h3>
 * <p>Constraints are declared in Hive TBLPROPERTIES using the following keys:</p>
 *
 * <h4>Primary Key:</h4>
 * <pre>
 *   TBLPROPERTIES ('primary.keys' = 'col1,col2')
 * </pre>
 *
 * <h4>Unique Keys (single):</h4>
 * <pre>
 *   TBLPROPERTIES ('unique.keys' = 'col1,col2')
 * </pre>
 *
 * <h4>Unique Keys (multiple):</h4>
 * <pre>
 *   TBLPROPERTIES (
 *     'unique.keys.0' = 'email',
 *     'unique.keys.1' = 'phone,country_code'
 *   )
 * </pre>
 *
 * <h3>Hive DDL Example:</h3>
 * <pre>{@code
 * -- Create table with constraints declared in properties
 * CREATE EXTERNAL TABLE orders (
 *     order_id    BIGINT,
 *     customer_id BIGINT,
 *     email       STRING,
 *     amount      DECIMAL(10,2)
 * )
 * STORED AS ORC
 * TBLPROPERTIES (
 *     'primary.keys' = 'order_id',
 *     'unique.keys.0' = 'email',
 *     'unique.keys.1' = 'customer_id'
 * );
 *
 * -- Or add later via ALTER TABLE
 * ALTER TABLE orders SET TBLPROPERTIES ('primary.keys' = 'order_id');
 * ALTER TABLE orders SET TBLPROPERTIES ('unique.keys' = 'email');
 * }</pre>
 *
 * <h3>Usage in Doris:</h3>
 * <p>After setting these properties, Doris will automatically detect PK/UK constraints
 * when reading the Hive table metadata. This enables:</p>
 * <ul>
 *   <li>MV rewriting with join elimination (extra joins on unique keys)</li>
 *   <li>Query optimization based on uniqueness guarantees</li>
 *   <li>Foreign key constraint validation</li>
 * </ul>
 */
public class HiveConstraintExtractor {

    /** Property key for primary key columns. Value: comma-separated column names. */
    public static final String PRIMARY_KEY_PROPERTY = "primary.keys";

    /** Property key for a single unique key. Value: comma-separated column names. */
    public static final String UNIQUE_KEY_PROPERTY = "unique.keys";

    /**
     * Property key prefix for multiple unique keys.
     * Append ".0", ".1", etc. for each unique key.
     * Value: comma-separated column names for each key.
     */
    public static final String UNIQUE_KEY_PREFIX = "unique.keys.";

    // ========================= Data Classes =========================

    /**
     * Represents a primary key constraint extracted from Hive properties.
     */
    public static class HivePrimaryKey {
        private final String constraintName;
        private final List<String> columns;

        public HivePrimaryKey(String constraintName, List<String> columns) {
            this.constraintName = constraintName;
            this.columns = ImmutableList.copyOf(columns);
        }

        public String getConstraintName() {
            return constraintName;
        }

        public List<String> getColumns() {
            return columns;
        }

        public Set<String> getColumnSet() {
            return ImmutableSet.copyOf(columns);
        }

        @Override
        public String toString() {
            return "HivePrimaryKey(" + constraintName + ": " + columns + ")";
        }
    }

    /**
     * Represents a unique key constraint extracted from Hive properties.
     */
    public static class HiveUniqueKey {
        private final String constraintName;
        private final List<String> columns;

        public HiveUniqueKey(String constraintName, List<String> columns) {
            this.constraintName = constraintName;
            this.columns = ImmutableList.copyOf(columns);
        }

        public String getConstraintName() {
            return constraintName;
        }

        public List<String> getColumns() {
            return columns;
        }

        public Set<String> getColumnSet() {
            return ImmutableSet.copyOf(columns);
        }

        @Override
        public String toString() {
            return "HiveUniqueKey(" + constraintName + ": " + columns + ")";
        }
    }

    // ========================= Extraction Methods =========================
    /**
     * Extract primary key from Hive table parameters.
     */
    public static HivePrimaryKey extractPrimaryKey(String tableName, Map<String, String> parameters) {
        if (parameters == null) {
            return null;
        }

        String pkValue = parameters.get(PRIMARY_KEY_PROPERTY);
        if (pkValue == null || pkValue.trim().isEmpty()) {
            return null;
        }

        List<String> columns = parseColumnList(pkValue);
        if (columns.isEmpty()) {
            return null;
        }

        return new HivePrimaryKey("hive_pk_" + tableName, columns);
    }

    /**
     * Extract all unique keys from Hive table parameters.
     */
    public static List<HiveUniqueKey> extractUniqueKeys(String tableName, Map<String, String> parameters) {
        if (parameters == null) {
            return Collections.emptyList();
        }

        List<HiveUniqueKey> result = new ArrayList<>();

        // Check single unique key: unique.keys
        String singleUkValue = parameters.get(UNIQUE_KEY_PROPERTY);
        if (singleUkValue != null && !singleUkValue.trim().isEmpty()) {
            List<String> columns = parseColumnList(singleUkValue);
            if (!columns.isEmpty()) {
                result.add(new HiveUniqueKey("hive_uk_" + tableName, columns));
            }
        }

        // Check numbered unique keys: unique.keys.0, unique.keys.1, ...
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(UNIQUE_KEY_PREFIX)) {
                String suffix = key.substring(UNIQUE_KEY_PREFIX.length());
                List<String> columns = parseColumnList(entry.getValue());
                if (!columns.isEmpty()) {
                    result.add(new HiveUniqueKey("hive_uk_" + suffix + "_" + tableName, columns));
                }
            }
        }

        return result;
    }

    /**
     * Check if a Hive table has any constraint properties defined.
     */
    public static boolean hasConstraintProperties(Map<String, String> parameters) {
        if (parameters == null) {
            return false;
        }
        if (parameters.containsKey(PRIMARY_KEY_PROPERTY)) {
            return true;
        }
        if (parameters.containsKey(UNIQUE_KEY_PROPERTY)) {
            return true;
        }
        for (String key : parameters.keySet()) {
            if (key.startsWith(UNIQUE_KEY_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Parse a comma-separated column list, trimming whitespace and filtering empties.
     *
     * <p>Examples:
     * <ul>
     *   <li>"id" → ["id"]</li>
     *   <li>"col1, col2" → ["col1", "col2"]</li>
     *   <li>"  col1 , col2 , col3  " → ["col1", "col2", "col3"]</li>
     *   <li>"" → []</li>
     *   <li>null → []</li>
     * </ul>
     */
    static List<String> parseColumnList(String value) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.stream(value.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());
    }
}
