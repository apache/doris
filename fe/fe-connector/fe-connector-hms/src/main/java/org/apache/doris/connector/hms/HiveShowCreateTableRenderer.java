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

package org.apache.doris.connector.hms;

import org.apache.doris.connector.api.ConnectorColumn;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Renders the native hive {@code SHOW CREATE TABLE} DDL for a base table — a byte-for-byte connector-side port of
 * legacy {@code HiveMetaStoreClientHelper.showCreateTable} (base-table branch, L749-824). It lives connector-side
 * (not fe-core) because it emits hive-format specifics — {@code ROW FORMAT SERDE}, {@code WITH SERDEPROPERTIES},
 * {@code STORED AS INPUTFORMAT/OUTPUTFORMAT} — that fe-core must not know about (iron rule).
 *
 * <p>Two quoting conventions are load-bearing and must both be preserved (the acceptance suites discriminate on
 * them): {@code WITH SERDEPROPERTIES ('k' = 'v')} has SPACES around {@code =}, while {@code TBLPROPERTIES
 * ('k'='v')} has NONE. Column comments are emitted only when non-null (an empty {@code COMMENT ''} would break the
 * exact substring the meta-cache suite asserts), and the single {@code comment} table param is lifted out to the
 * top-level {@code COMMENT} clause rather than left in {@code TBLPROPERTIES}.
 *
 * <p>Column/partition types are reconstructed from the mapped {@link org.apache.doris.connector.api.ConnectorType}
 * via {@link HmsTypeMapping#toHiveTypeString} (the SPI carries the mapped type, not the raw thrift string). That
 * round-trip is exact for scalars/decimal/date/timestamp/nested; a raw HMS {@code varchar(n)} would render as
 * {@code string} (length dropped), harmless here because such columns are stored as {@code string} in HMS. An
 * unmappable type throws {@code IllegalArgumentException} — legacy could not hit this (it echoed the raw string),
 * so it is left to fail loud rather than guessed at (Rule 2). This method must stay reflection-free: the caller
 * runs it on the fe-core thread AFTER the pinned metastore fetch, so any future name-based reflection here would
 * need its own TCCL pin.
 */
public final class HiveShowCreateTableRenderer {

    /** HMS table-param key carrying the table comment (lifted to the top-level COMMENT clause). */
    private static final String COMMENT_KEY = "comment";

    private HiveShowCreateTableRenderer() {
    }

    public static String render(HmsTableInfo table) {
        StringBuilder output = new StringBuilder();
        output.append(String.format("CREATE TABLE `%s`(\n", table.getTableName()));

        List<ConnectorColumn> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            ConnectorColumn col = columns.get(i);
            // 2-space indent for data columns (partition keys below use 1-space, matching legacy).
            output.append(String.format("  `%s` %s", col.getName(),
                    HmsTypeMapping.toHiveTypeString(col.getType())));
            if (col.getComment() != null) {
                output.append(String.format(" COMMENT '%s'", col.getComment()));
            }
            if (i < columns.size() - 1) {
                output.append(",\n");
            }
        }
        output.append(")\n");

        Map<String, String> params = table.getParameters();
        if (params != null && params.containsKey(COMMENT_KEY)) {
            output.append(String.format("COMMENT '%s'", params.get(COMMENT_KEY))).append("\n");
        }

        List<ConnectorColumn> partitionKeys = table.getPartitionKeys();
        if (partitionKeys != null && !partitionKeys.isEmpty()) {
            output.append("PARTITIONED BY (\n")
                    .append(partitionKeys.stream()
                            .map(p -> String.format(" `%s` %s", p.getName(),
                                    HmsTypeMapping.toHiveTypeString(p.getType())))
                            .collect(Collectors.joining(",\n")))
                    .append(")\n");
        }

        List<String> bucketCols = table.getBucketCols();
        if (bucketCols != null && !bucketCols.isEmpty()) {
            output.append("CLUSTERED BY (\n")
                    .append(bucketCols.stream().map(c -> "  " + c).collect(Collectors.joining(",\n")))
                    .append(")\n")
                    .append(String.format("INTO %d BUCKETS\n", table.getNumBuckets()));
        }

        String serdeLib = table.getSerializationLib();
        if (serdeLib != null && !serdeLib.isEmpty()) {
            output.append("ROW FORMAT SERDE\n").append(String.format("  '%s'\n", serdeLib));
        }

        Map<String, String> serdeParams = table.getSdParameters();
        if (serdeParams != null && !serdeParams.isEmpty()) {
            // SERDEPROPERTIES: spaces around '=' (legacy L789).
            output.append("WITH SERDEPROPERTIES (\n")
                    .append(serdeParams.entrySet().stream()
                            .map(e -> String.format("  '%s' = '%s'", e.getKey(), e.getValue()))
                            .collect(Collectors.joining(",\n")))
                    .append(")\n");
        }

        String inputFormat = table.getInputFormat();
        if (inputFormat != null && !inputFormat.isEmpty()) {
            output.append("STORED AS INPUTFORMAT\n").append(String.format("  '%s'\n", inputFormat));
        }
        String outputFormat = table.getOutputFormat();
        if (outputFormat != null && !outputFormat.isEmpty()) {
            output.append("OUTPUTFORMAT\n").append(String.format("  '%s'\n", outputFormat));
        }
        String location = table.getLocation();
        if (location != null && !location.isEmpty()) {
            output.append("LOCATION\n").append(String.format("  '%s'\n", location));
        }

        if (params != null && !params.isEmpty()) {
            // TBLPROPERTIES: no spaces around '=' (legacy L818); the comment key is lifted to COMMENT above.
            Map<String, String> tblProps = new LinkedHashMap<>(params);
            tblProps.remove(COMMENT_KEY);
            if (!tblProps.isEmpty()) {
                output.append("TBLPROPERTIES (\n")
                        .append(tblProps.entrySet().stream()
                                .map(e -> String.format("  '%s'='%s'", e.getKey(), e.getValue()))
                                .collect(Collectors.joining(",\n")))
                        .append(")");
            }
        }
        return output.toString();
    }
}
