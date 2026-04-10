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

import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.hive.HiveExternalMetaCache;

import com.google.common.collect.Lists;

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
        return parseColumnsFromPath(filePath, columnsFromPath, true, false);
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
        if (columnsFromPath == null || columnsFromPath.isEmpty()) {
            return Collections.emptyList();
        }
        // ACID paths have one extra level: table/par=val/delta_xxx/file → pathCount = 3
        int pathCount = isACID ? 3 : 2;
        if (!caseSensitive) {
            for (int i = 0; i < columnsFromPath.size(); i++) {
                String path = columnsFromPath.remove(i);
                columnsFromPath.add(i, path.toLowerCase());
            }
        }
        String[] strings = filePath.split("/");
        if (strings.length < 2) {
            throw new UserException("Fail to parse columnsFromPath, expected: "
                    + columnsFromPath + ", filePath: " + filePath);
        }
        String[] columns = new String[columnsFromPath.size()];
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
                        + columnsFromPath + ", filePath: " + filePath);
            }
            skipOnce = false;
            String[] pair = str.split("=", 2);
            if (pair.length != 2) {
                throw new UserException("Fail to parse columnsFromPath, expected: "
                        + columnsFromPath + ", filePath: " + filePath);
            }
            String parsedColumnName = caseSensitive ? pair[0] : pair[0].toLowerCase();
            int index = columnsFromPath.indexOf(parsedColumnName);
            if (index == -1) {
                continue;
            }
            columns[index] = HiveExternalMetaCache.HIVE_DEFAULT_PARTITION.equals(pair[1])
                ? FeConstants.null_string : pair[1];
            size++;
            if (size >= columnsFromPath.size()) {
                break;
            }
        }
        if (size != columnsFromPath.size()) {
            throw new UserException("Fail to parse columnsFromPath, expected: "
                    + columnsFromPath + ", filePath: " + filePath);
        }
        return Lists.newArrayList(columns);
    }
}
