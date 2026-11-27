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


package org.apache.doris.connector.deltalake;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.types.StructType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

final class DeltaLakeTestTables {
    private DeltaLakeTestTables() {
    }

    static Path commit(Path table, long version, String id, StructType schema) throws IOException {
        return commit(table, version, id, schema, Collections.emptyList());
    }

    static Path commit(Path table, long version, String id, StructType schema, List<String> partitions)
            throws IOException {
        return commit(table, version, id, schema, partitions, Map.of("minReaderVersion", 1, "minWriterVersion", 2));
    }

    static Path commit(Path table, long version, String id, StructType schema, List<String> partitions,
            Map<String, Object> protocolFields) throws IOException {
        return commit(table, version, id, schema, partitions, protocolFields, Collections.emptyMap(), null, null);
    }

    static Path commit(Path table, long version, String id, StructType schema, List<String> partitions,
            Map<String, Object> protocolFields, Map<String, String> configuration, Long inCommitTimestamp,
            Long fileModifiedMillis) throws IOException {
        Path log = Files.createDirectories(table.resolve("_delta_log"));
        Path commitFile = log.resolve(String.format("%020d.json", version));
        return writeCommit(commitFile, id, schema, partitions, protocolFields, configuration, inCommitTimestamp,
                fileModifiedMillis);
    }

    static Path writeCommit(Path commitFile, String id, StructType schema, List<String> partitions,
            Map<String, Object> protocolFields, Map<String, String> configuration, Long inCommitTimestamp,
            Long fileModifiedMillis) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Files.createDirectories(commitFile.getParent());
        StringBuilder content = new StringBuilder();
        if (inCommitTimestamp != null) {
            content.append(mapper.writeValueAsString(Map.of("commitInfo",
                    Map.of("timestamp", inCommitTimestamp, "inCommitTimestamp", inCommitTimestamp)))).append('\n');
        }
        content.append(mapper.writeValueAsString(Map.of("protocol", protocolFields))).append('\n');
        content.append(mapper.writeValueAsString(Map.of("metaData", Map.of(
                "id", id, "format", Map.of("provider", "parquet", "options", Collections.emptyMap()),
                "schemaString", schema.toJson(), "partitionColumns", partitions,
                "configuration", configuration, "createdTime", 0)))).append('\n');
        Files.writeString(commitFile, content.toString());
        if (fileModifiedMillis != null) {
            Files.setLastModifiedTime(commitFile, FileTime.fromMillis(fileModifiedMillis));
        }
        return commitFile;
    }

    static void checksum(Path table, long version, long files, long bytes) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        List<String> actions = Files.readAllLines(table.resolve("_delta_log/00000000000000000000.json"));
        Files.writeString(table.resolve(String.format("_delta_log/%020d.crc", version)),
                mapper.writeValueAsString(Map.of(
                    "tableSizeBytes", bytes, "numFiles", files, "numMetadata", 1, "numProtocol", 1,
                    "metadata", mapper.readTree(actions.get(1)).get("metaData"),
                    "protocol", mapper.readTree(actions.get(0)).get("protocol"))));
    }
}
