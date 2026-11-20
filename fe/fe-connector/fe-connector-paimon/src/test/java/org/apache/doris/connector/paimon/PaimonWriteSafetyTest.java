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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.ResolvingFileIO;
import org.apache.paimon.fs.hadoop.HadoopFileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.hive.HiveCatalogLockFactory;
import org.apache.paimon.jdbc.JdbcCatalogLockFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PaimonWriteSafetyTest {
    private static final PaimonCatalogProperties DEFAULTS = PaimonCatalogProperties.of(Map.of());

    @Test
    public void localAndObjectStorageNeedProtectionEvenWithLockEnabled() {
        for (String scheme : List.of("file", "s3", "oss", "viewfs")) {
            FileStoreTable table = table(scheme, LocalFileIO.create(), CatalogEnvironment.empty());
            PaimonCatalogProperties properties = PaimonCatalogProperties.of(Map.of("paimon.lock.enabled", "true"));
            DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                    () -> PaimonWriteSafety.validate(table, properties));
            Assertions.assertTrue(failure.getMessage().contains("paimon.allow-unsafe-writes"));
            Assertions.assertTrue(failure.getMessage().contains("compaction"));
        }
    }

    @Test
    public void hdfsRequiresTheHadoopImplementation() {
        Assertions.assertNull(PaimonWriteSafety.rejectionReason(table("hdfs",
                new HadoopFileIO(new Path("hdfs://namenode/warehouse/db.db/tbl")), CatalogEnvironment.empty())));
        Assertions.assertNotNull(PaimonWriteSafety.rejectionReason(
                table("hdfs", LocalFileIO.create(), CatalogEnvironment.empty())));
    }

    @Test
    public void resolvingFileIoUsesTheTargetPath() {
        ResolvingFileIO io = new ResolvingFileIO();
        io.configure(CatalogContext.create(new Options()));
        Assertions.assertNull(PaimonWriteSafety.rejectionReason(table("hdfs", io, CatalogEnvironment.empty())));
        Assertions.assertNotNull(PaimonWriteSafety.rejectionReason(table("file", io, CatalogEnvironment.empty())));
    }

    @Test
    public void unknownLockFactoriesDoNotImplyProtection() {
        CatalogLockFactory unknown = new CatalogLockFactory() {
            @Override
            public String identifier() {
                return "unverified";
            }

            @Override
            public CatalogLock createLock(CatalogLockContext context) {
                throw new AssertionError("admission must not instantiate unknown locks");
            }
        };
        Assertions.assertTrue(PaimonWriteSafety.rejectionReason(
                table("s3", LocalFileIO.create(), environment(unknown))).contains("lock=unverified"));
    }

    @Test
    public void versionManagementRequiresTheCatalogLoaderToo() {
        CatalogEnvironment catalogCommit = new CatalogEnvironment(Identifier.create("db", "tbl"),
                "uuid", () -> {
                    throw new AssertionError("admission must not load the catalog");
                },
                null, null, null, true, false);
        Assertions.assertNull(PaimonWriteSafety.rejectionReason(table("s3", LocalFileIO.create(), catalogCommit)));
        CatalogEnvironment missingLoader = new CatalogEnvironment(Identifier.create("db", "tbl"),
                "uuid", null, null, null, null, true, false);
        Assertions.assertNotNull(PaimonWriteSafety.rejectionReason(table("s3", LocalFileIO.create(), missingLoader)));
    }

    @Test
    public void hiveLockIsAcceptedButJdbcLeaseIsNot() {
        Assertions.assertNull(PaimonWriteSafety.rejectionReason(
                table("s3", LocalFileIO.create(), environment(new HiveCatalogLockFactory()))));
        Assertions.assertTrue(PaimonWriteSafety.rejectionReason(
                table("s3", LocalFileIO.create(), environment(new JdbcCatalogLockFactory())))
                .contains("expired lock holder"));
    }

    @Test
    public void optInAllowsUnsupportedPublicationAndCanBeDisabledAgain() {
        FileStoreTable table = table("file", LocalFileIO.create(), CatalogEnvironment.empty());
        Assertions.assertDoesNotThrow(() -> PaimonWriteSafety.validate(table,
                PaimonCatalogProperties.of(Map.of(PaimonCatalogProperties.ALLOW_UNSAFE_WRITES, "true"))));
        Assertions.assertThrows(DorisConnectorException.class, () -> PaimonWriteSafety.validate(table,
                PaimonCatalogProperties.of(Map.of(PaimonCatalogProperties.ALLOW_UNSAFE_WRITES, "false"))));
        Assertions.assertThrows(DorisConnectorException.class, () -> PaimonWriteSafety.validate(table, DEFAULTS));
    }

    @Test
    public void everyDmlIsRejectedBeforeTransactionBindingOrSinkCreation() {
        FileStoreTable table = table("file", LocalFileIO.create(), CatalogEnvironment.empty());
        PaimonTableHandle tableHandle = new PaimonTableHandle("db", "tbl", List.of(), List.of());
        tableHandle.setPaimonTable(table);
        RecordingConnectorContext context = new RecordingConnectorContext();
        PaimonWritePlanProvider provider = new PaimonWritePlanProvider(DEFAULTS, null, context);
        for (WriteOperation operation : provider.supportedOperations()) {
            ConnectorWriteHandle handle = new ConnectorWriteHandle() {
                @Override
                public ConnectorTableHandle getTableHandle() {
                    return tableHandle;
                }

                @Override
                public List<ConnectorColumn> getColumns() {
                    return List.of();
                }

                @Override
                public boolean isOverwrite() {
                    return operation == WriteOperation.OVERWRITE;
                }

                @Override
                public Map<String, String> getStaticPartitionSpec() {
                    return Map.of();
                }

                @Override
                public WriteOperation getWriteOperation() {
                    return operation;
                }

                @Override
                public int getBeExecVersion() {
                    return PaimonWritePlanProvider.MIN_BE_EXEC_VERSION;
                }
            };
            Assertions.assertTrue(Assertions.assertThrows(DorisConnectorException.class,
                    () -> provider.planWrite(null, handle)).getMessage().contains("snapshot publication"));
        }
        Assertions.assertEquals(provider.supportedOperations().size(), context.authCount);
    }

    private static CatalogEnvironment environment(CatalogLockFactory factory) {
        return new CatalogEnvironment(Identifier.create("db", "tbl"), null, null, factory, null, null, false, false);
    }

    private static FileStoreTable table(String scheme, FileIO io, CatalogEnvironment environment) {
        return FileStoreTableFactory.create(io, new Path(scheme + ":///warehouse/db.db/tbl"),
                new TableSchema(0, List.of(DataTypes.FIELD(0, "id", DataTypes.INT())), 0,
                        Collections.emptyList(), Collections.emptyList(), Map.of(), ""), environment);
    }
}
