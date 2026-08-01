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

package org.apache.doris.connector.iceberg.action;

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.procedure.ConnectorProcedureResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class IcebergRemoveOrphanFilesActionTest {
    private static final long MIN_RETENTION_MS = Duration.ofHours(24).toMillis();

    @Test
    public void gcDisabledPreventsDeletion(@TempDir Path temp) throws Exception {
        Table table = createTable(temp.resolve("table"),
                Collections.singletonMap(TableProperties.GC_ENABLED, "false"));
        Path orphan = createOldFile(temp.resolve("table/data/orphan.parquet"));
        IcebergRemoveOrphanFilesAction action = action(System.currentTimeMillis() - MIN_RETENTION_MS, false);
        action.validate();

        Assertions.assertThrows(DorisConnectorException.class,
                () -> action.execute(table, ActionTestTables.session("UTC")));
        Assertions.assertTrue(Files.exists(orphan));
    }

    @Test
    public void recentCutoffCannotRaceAnUncommittedWriter(@TempDir Path temp) throws Exception {
        Table table = createTable(temp.resolve("table"), Collections.emptyMap());
        Path uncommitted = createOldFile(temp.resolve("table/data/uncommitted.parquet"));
        IcebergRemoveOrphanFilesAction action = action(System.currentTimeMillis(), false);
        action.validate();

        Assertions.assertThrows(DorisConnectorException.class,
                () -> action.execute(table, ActionTestTables.session("UTC")));
        Assertions.assertTrue(Files.exists(uncommitted));
    }

    @Test
    public void keepsVersionHintWhileDeletingAnOldOrphan(@TempDir Path temp) throws Exception {
        Table table = createTable(temp.resolve("table"), Collections.emptyMap());
        Path orphan = createOldFile(temp.resolve("table/data/orphan.parquet"));
        Path versionHint = Path.of(java.net.URI.create(ReachableFileUtil.versionHintLocation(table)));
        Files.setLastModifiedTime(versionHint, FileTime.fromMillis(1));
        IcebergRemoveOrphanFilesAction action = action(System.currentTimeMillis() - MIN_RETENTION_MS, false);
        action.validate();

        ConnectorProcedureResult result = action.execute(table, ActionTestTables.session("UTC"));

        Assertions.assertEquals("1", result.getRows().get(0).get(0));
        Assertions.assertEquals("1", result.getRows().get(0).get(1));
        Assertions.assertFalse(Files.exists(orphan));
        Assertions.assertTrue(Files.exists(versionHint));
    }

    @Test
    public void treatsS3SchemeAliasesAsTheSameFile() {
        Assertions.assertTrue(IcebergRemoveOrphanFilesAction.sameFileIdentity(
                "s3://bucket/path/data.parquet", "s3a://bucket/path/data.parquet"));
        Assertions.assertTrue(IcebergRemoveOrphanFilesAction.sameFileIdentity(
                "s3n://bucket/path/data.parquet", "s3://BUCKET/path/data.parquet"));
    }

    @Test
    public void rejectsUnresolvedPrefixMismatches() {
        Assertions.assertThrows(DorisConnectorException.class,
                () -> IcebergRemoveOrphanFilesAction.verifyNoPrefixMismatch(
                        "s3://first/path/data.parquet",
                        Collections.singleton("s3://second/path/data.parquet")));
    }

    private static IcebergRemoveOrphanFilesAction action(long olderThan, boolean dryRun) {
        Map<String, String> properties = new HashMap<>();
        properties.put(IcebergRemoveOrphanFilesAction.OLDER_THAN, String.valueOf(olderThan));
        properties.put(IcebergRemoveOrphanFilesAction.DRY_RUN, String.valueOf(dryRun));
        return new IcebergRemoveOrphanFilesAction(properties, Collections.emptyList(), null);
    }

    private static Table createTable(Path location, Map<String, String> properties) {
        HadoopTables tables = new HadoopTables(new Configuration());
        return tables.create(ActionTestTables.SCHEMA, PartitionSpec.unpartitioned(), properties,
                location.toUri().toString());
    }

    private static Path createOldFile(Path path) throws Exception {
        Files.createDirectories(path.getParent());
        Files.write(path, new byte[] {1});
        Files.setLastModifiedTime(path, FileTime.fromMillis(1));
        return path;
    }
}
