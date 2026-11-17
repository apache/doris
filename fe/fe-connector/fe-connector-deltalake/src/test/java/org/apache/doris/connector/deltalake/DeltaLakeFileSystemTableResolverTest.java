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

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

class DeltaLakeFileSystemTableResolverTest {
    @TempDir
    Path warehouse;

    private final DeltaLakeFileIO fileIO = new DeltaLakeFileIO(new Configuration(), new RecordingConnectorContext());

    @AfterEach
    void close() throws Exception {
        fileIO.close();
    }

    @Test
    void listsOnlyImmediateDatabasesAndDeltaTables() throws Exception {
        Files.createDirectories(warehouse.resolve("db/table/_delta_log"));
        Files.createDirectories(warehouse.resolve("db/plain_parquet"));
        Files.createDirectories(warehouse.resolve("other"));
        Files.write(warehouse.resolve("README"), new byte[0]);
        Files.write(warehouse.resolve("db/not_a_table"), new byte[0]);
        DeltaLakeFileSystemTableResolver resolver = resolver();
        Assertions.assertEquals(Arrays.asList("db", "other"), resolver.listDatabases());
        Assertions.assertEquals(Collections.singletonList("table"), resolver.listTables("db"));
        Assertions.assertTrue(resolver.tableLocation("db", "table").isPresent());
        Assertions.assertTrue(resolver.tableLocation("db", "plain_parquet").isEmpty());
        Assertions.assertTrue(resolver.tableLocation("db", "missing").isEmpty());
    }

    @Test
    void emptyAndMissingNamespacesAreEmpty() throws Exception {
        Assertions.assertTrue(resolver().listDatabases().isEmpty());
        Assertions.assertTrue(resolver().listTables("missing").isEmpty());
        Assertions.assertTrue(new DeltaLakeFileSystemTableResolver(
                warehouse.resolve("not_created").toUri().toString(), fileIO).listDatabases().isEmpty());
    }

    @Test
    void rejectsLogFilesAndNamesThatReplaceWarehouse() throws Exception {
        Files.createDirectories(warehouse.resolve("db/broken"));
        Files.write(warehouse.resolve("db/broken/_delta_log"), new byte[0]);
        DeltaLakeFileSystemTableResolver resolver = resolver();
        Assertions.assertTrue(resolver.tableLocation("db", "broken").isEmpty());
        for (String name : Arrays.asList("", ".", "..", "../db", "/tmp", "s3://bucket", "a\\b")) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> resolver.listTables(name));
            Assertions.assertThrows(IllegalArgumentException.class, () -> resolver.tableLocation("db", name));
        }
    }

    private DeltaLakeFileSystemTableResolver resolver() {
        return new DeltaLakeFileSystemTableResolver(warehouse.toUri().toString(), fileIO);
    }
}
