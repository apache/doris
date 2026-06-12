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

package org.apache.doris.datasource;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.hive.HiveExternalMetaCache;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility methods for parsing partition column values from Hive-style file paths.
 *
 * <p>Hive partitioned tables encode partition key-value pairs in the directory path
 * (e.g. {@code hdfs://nn/tbl/year=2024/month=01/file.parquet}).  These helpers extract
 * the column values in the order given by {@code columnsFromPath}.
 */
public final class FilePartitionUtils {

    private FilePartitionUtils() {}

    public static final class ParsedColumnsFromPath {
        private final List<String> values;
        private final List<Boolean> isNull;

        private ParsedColumnsFromPath(List<String> values, List<Boolean> isNull) {
            this.values = values;
            this.isNull = isNull;
        }

        public List<String> getValues() {
            return values;
        }

        public List<Boolean> getIsNull() {
            return isNull;
        }
    }

    /**
     * Parses partition column values from a Hive-style file path using case-sensitive matching.
     *
     * @param filePath        absolute file path that contains partition segments
     * @param columnsFromPath ordered list of partition column names to extract
     * @return ordered list of partition values (parallel to {@code columnsFromPath})
     * @throws UserException if the path does not contain the expected partition segments
     */
    public static List<String> parseColumnsFromPath(String filePath, List<String> columnsFromPath)
            throws UserException {
        return parseColumnsFromPathWithNullInfo(filePath, columnsFromPath, true, false).getValues();
    }

    /**
     * Parses partition column values from a Hive-style file path.
     *
     * @param filePath        absolute file path that contains partition segments
     * @param columnsFromPath ordered list of partition column names to extract
     * @param caseSensitive   whether column name matching is case-sensitive
     * @param isACID          whether the path follows the ACID layout
     *                        ({@code table/par=val/delta_xxx/file}), which adds one extra path level
     * @return ordered list of partition values (parallel to {@code columnsFromPath})
     * @throws UserException if the path does not contain the expected partition segments
     */
    public static List<String> parseColumnsFromPath(
            String filePath,
            List<String> columnsFromPath,
            boolean caseSensitive,
            boolean isACID)
            throws UserException {
        return parseColumnsFromPathWithNullInfo(filePath, columnsFromPath, caseSensitive, isACID)
                .getValues();
    }

    public static ParsedColumnsFromPath parseColumnsFromPathWithNullInfo(
            String filePath,
            List<String> columnsFromPath,
            boolean caseSensitive,
            boolean isACID)
            throws UserException {
        if (columnsFromPath == null || columnsFromPath.isEmpty()) {
            return new ParsedColumnsFromPath(Collections.emptyList(), Collections.emptyList());
        }
        // ACID paths have one extra level: table/par=val/delta_xxx/file → pathCount = 3
        int pathCount = isACID ? 3 : 2;
        List<String> expectedColumns = columnsFromPath;
        if (!caseSensitive) {
            expectedColumns = new ArrayList<>(columnsFromPath.size());
            for (String path : columnsFromPath) {
                expectedColumns.add(path.toLowerCase());
            }
        }
        String[] strings = filePath.split("/");
        if (strings.length < 2) {
            throw new UserException("Fail to parse columnsFromPath, expected: "
                    + expectedColumns + ", filePath: " + filePath);
        }
        String[] columns = new String[expectedColumns.size()];
        Boolean[] columnValueIsNull = new Boolean[expectedColumns.size()];
        int size = 0;
        boolean skipOnce = true;
        for (int i = strings.length - pathCount; i >= 0; i--) {
            String str = strings[i];
            if (str != null && str.isEmpty()) {
                continue;
            }
            if (str == null || !str.contains("=")) {
                if (!isACID && skipOnce) {
                    skipOnce = false;
                    continue;
                }
                throw new UserException("Fail to parse columnsFromPath, expected: "
                        + expectedColumns + ", filePath: " + filePath);
            }
            skipOnce = false;
            String[] pair = str.split("=", 2);
            if (pair.length != 2) {
                throw new UserException("Fail to parse columnsFromPath, expected: "
                        + expectedColumns + ", filePath: " + filePath);
            }
            String parsedColumnName = caseSensitive ? pair[0] : pair[0].toLowerCase();
            int index = expectedColumns.indexOf(parsedColumnName);
            if (index == -1) {
                continue;
            }
            boolean isNull = HiveExternalMetaCache.HIVE_DEFAULT_PARTITION.equals(pair[1]);
            columns[index] = isNull ? "" : pair[1];
            columnValueIsNull[index] = isNull;
            size++;
            if (size >= expectedColumns.size()) {
                break;
            }
        }
        if (size != expectedColumns.size()) {
            throw new UserException("Fail to parse columnsFromPath, expected: "
                    + expectedColumns + ", filePath: " + filePath);
        }
        return new ParsedColumnsFromPath(Lists.newArrayList(columns), Lists.newArrayList(columnValueIsNull));
    }

    public static ParsedColumnsFromPath normalizeColumnsFromPath(List<String> columnsFromPath) {
        if (columnsFromPath == null || columnsFromPath.isEmpty()) {
            return new ParsedColumnsFromPath(Collections.emptyList(), Collections.emptyList());
        }
        List<String> values = new ArrayList<>(columnsFromPath.size());
        List<Boolean> isNull = new ArrayList<>(columnsFromPath.size());
        for (String value : columnsFromPath) {
            boolean nullValue = value == null || HiveExternalMetaCache.HIVE_DEFAULT_PARTITION.equals(value);
            values.add(nullValue ? "" : value);
            isNull.add(nullValue);
        }
        return new ParsedColumnsFromPath(values, isNull);
    }
}
