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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

/**
 * Verifies {@link IcebergAuthenticatedTableOperations} carries the authenticated {@link FileIO} through
 * iceberg's transaction plumbing — the route the Kerberos manifest writes actually take.
 *
 * <p>WHY it matters: {@code BaseTransaction.TransactionTableOperations} does NOT use the {@code io()} of the
 * ops handed to {@code Transactions.newTransaction}. Its {@code io()} returns {@code tempOps.io()} where
 * {@code tempOps = ops.temp(current)} (re-created again on every intermediate commit), and
 * {@code SnapshotProducer.newManifestOutputFile} — the worker-pool manifest write, the exact site of the
 * {@code Client cannot authenticate via:[TOKEN, KERBEROS]} CI failure
 * (test_iceberg_hadoop_catalog_kerberos INSERT) — takes its {@code OutputFile} from that {@code io()}.
 * A {@code temp()} that forwards to the raw delegate (e.g. {@code HadoopTableOperations.temp()}, whose
 * result's {@code io()} is the raw unauthenticated {@code HadoopFileIO}) silently bypasses the whole wrap:
 * red here means the Kerberos INSERT regresses even though every direct {@code io()} call looks wrapped.
 */
public class IcebergAuthenticatedTableOperationsTest {

    @Test
    public void transactionOpsExposeAuthenticatedIo() {
        FileIO rawIo = new MarkerFileIO();
        FileIO authIo = new MarkerFileIO();
        FakeCatalogOps delegate = new FakeCatalogOps(newMetadata(), rawIo);
        TableOperations authOps = new IcebergAuthenticatedTableOperations(delegate, authIo);

        // Mirrors IcebergConnectorTransaction.openTransaction. BaseTransaction routes its io() through
        // ops.temp(current), so this is the assertion that guards the worker-pool manifest write path.
        Transaction txn = Transactions.newTransaction("tbl", authOps);

        Assertions.assertSame(authIo, txn.table().io(),
                "transaction io() must be the authenticated FileIO; the raw io here means temp() leaked the "
                        + "unauthenticated delegate into the transaction and manifest writes lose the Kerberos doAs");
    }

    @Test
    public void tempWrapsDelegateTempNotTheBaseDelegate() {
        FileIO rawIo = new MarkerFileIO();
        FileIO authIo = new MarkerFileIO();
        TableMetadata metadata = newMetadata();
        FakeCatalogOps delegate = new FakeCatalogOps(metadata, rawIo);
        TableOperations authOps = new IcebergAuthenticatedTableOperations(delegate, authIo);

        TableOperations tempOps = authOps.temp(metadata);

        Assertions.assertSame(authIo, tempOps.io(),
                "temp ops must expose the authenticated FileIO — BaseTransaction reads io() from temp ops only");
        Assertions.assertEquals("temp:m.avro", tempOps.metadataFileLocation("m.avro"),
                "non-io calls must reach the DELEGATE's temp ops (uncommitted-metadata semantics), "
                        + "not the base delegate");
    }

    private static TableMetadata newMetadata() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        return TableMetadata.newTableMetadata(
                schema, PartitionSpec.unpartitioned(), "file:///tmp/iceberg-auth-ops-test", Collections.emptyMap());
    }

    /** Distinct no-op FileIO instances used purely as identity markers. */
    private static final class MarkerFileIO implements FileIO {
        @Override
        public InputFile newInputFile(String path) {
            throw new UnsupportedOperationException("marker only");
        }

        @Override
        public OutputFile newOutputFile(String path) {
            throw new UnsupportedOperationException("marker only");
        }

        @Override
        public void deleteFile(String path) {
            throw new UnsupportedOperationException("marker only");
        }

        @Override
        public Map<String, String> properties() {
            return Collections.emptyMap();
        }
    }

    /**
     * Catalog-ops double mirroring {@code HadoopTableOperations}: {@code temp()} returns a DISTINCT ops over the
     * uncommitted metadata whose {@code io()} is the same raw (unauthenticated) FileIO — the exact shape that
     * bypassed the wrap in production.
     */
    private static final class FakeCatalogOps implements TableOperations {
        private final TableMetadata metadata;
        private final FileIO rawIo;

        FakeCatalogOps(TableMetadata metadata, FileIO rawIo) {
            this.metadata = metadata;
            this.rawIo = rawIo;
        }

        @Override
        public TableMetadata current() {
            return metadata;
        }

        @Override
        public TableMetadata refresh() {
            return metadata;
        }

        @Override
        public void commit(TableMetadata base, TableMetadata newMetadata) {
            throw new UnsupportedOperationException("not exercised");
        }

        @Override
        public FileIO io() {
            return rawIo;
        }

        @Override
        public String metadataFileLocation(String fileName) {
            return "base:" + fileName;
        }

        @Override
        public LocationProvider locationProvider() {
            throw new UnsupportedOperationException("not exercised");
        }

        @Override
        public TableOperations temp(TableMetadata uncommittedMetadata) {
            return new TableOperations() {
                @Override
                public TableMetadata current() {
                    return uncommittedMetadata;
                }

                @Override
                public TableMetadata refresh() {
                    throw new UnsupportedOperationException("temp ops never refresh");
                }

                @Override
                public void commit(TableMetadata base, TableMetadata newMetadata) {
                    throw new UnsupportedOperationException("temp ops never commit");
                }

                @Override
                public FileIO io() {
                    return rawIo;
                }

                @Override
                public String metadataFileLocation(String fileName) {
                    return "temp:" + fileName;
                }

                @Override
                public LocationProvider locationProvider() {
                    throw new UnsupportedOperationException("not exercised");
                }
            };
        }
    }
}
