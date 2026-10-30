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
import java.util.Collections;
import java.util.List;
import java.util.Map;

final class DeltaLakeTestTables {
    private DeltaLakeTestTables() {
    }

    static void commit(Path table, long version, String id, StructType schema) throws IOException {
        commit(table, version, id, schema, Collections.emptyList());
    }

    static void commit(Path table, long version, String id, StructType schema, List<String> partitions)
            throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Path log = Files.createDirectories(table.resolve("_delta_log"));
        String protocol = mapper.writeValueAsString(Map.of("protocol",
                Map.of("minReaderVersion", 1, "minWriterVersion", 2)));
        String metadata = mapper.writeValueAsString(Map.of("metaData", Map.of(
                "id", id, "format", Map.of("provider", "parquet", "options", Collections.emptyMap()),
                "schemaString", schema.toJson(), "partitionColumns", partitions,
                "configuration", Collections.emptyMap(), "createdTime", 0)));
        Files.writeString(log.resolve(String.format("%020d.json", version)), protocol + "\n" + metadata + "\n");
    }
}
