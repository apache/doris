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

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Real SDK encrypted manifests with plaintext data descriptors isolate metadata authorization. */
final class EncryptedManifestFixture implements TableOperations, AutoCloseable {
    final AtomicBoolean denied = new AtomicBoolean();
    final AtomicInteger unwraps = new AtomicInteger();
    private final InMemoryFileIO raw = new InMemoryFileIO() {
        @Override
        public Map<String, String> properties() {
            return Collections.singletonMap(CatalogProperties.IO_MANIFEST_CACHE_ENABLED, "true");
        }
    };
    private final StandardEncryptionManager writer = manager(Collections.emptyList());
    private final FileIO writerIo = EncryptingFileIO.combine(raw, writer);
    private TableMetadata metadata = TableMetadata.newTableMetadata(
            new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
            PartitionSpec.unpartitioned(), SortOrder.unsorted(), "memory://encrypted-cache-" + UUID.randomUUID(),
            Map.of(TableProperties.FORMAT_VERSION, "3", TableProperties.ENCRYPTION_TABLE_KEY, "fixture-master-key"));

    EncryptedManifestFixture() {
        new BaseTable(this, "writer").newFastAppend().appendFile(
                DataFiles.builder(PartitionSpec.unpartitioned()).withPath(metadata.location() + "/plain.parquet")
                        .withFileSizeInBytes(100).withRecordCount(1).build()).commit();
    }

    void appendPlainFile(String name) {
        new BaseTable(this, "writer").newFastAppend().appendFile(
                DataFiles.builder(PartitionSpec.unpartitioned()).withPath(metadata.location() + "/" + name + ".parquet")
                        .withFileSizeInBytes(100).withRecordCount(1).build()).commit();
    }

    Table queryTable() {
        // Reparse so a query cannot inherit the writer's already-decrypted manifest-list descriptors.
        TableMetadata snapshot = TableMetadataParser.fromJson(TableMetadataParser.toJson(metadata));
        StandardEncryptionManager reader = manager(snapshot.encryptionKeys());
        FileIO readerIo = EncryptingFileIO.combine(raw, reader);
        return new BaseTable(new StaticTableOperations(snapshot, readerIo) {
            @Override
            public EncryptionManager encryption() {
                return reader;
            }
        }, "reader");
    }

    private StandardEncryptionManager manager(List<EncryptedKey> keys) {
        return new StandardEncryptionManager(keys, "fixture-master-key", 16, new KeyManagementClient() {
            @Override
            public void initialize(Map<String, String> properties) {
                // The fixture constructs its KMS directly instead of using the reflective SDK factory.
            }

            @Override
            public ByteBuffer wrapKey(ByteBuffer key, String keyId) {
                // A no-op test KMS isolates cache authorization; this is never a production wrapping scheme.
                return key.duplicate();
            }

            @Override
            public ByteBuffer unwrapKey(ByteBuffer key, String keyId) {
                unwraps.incrementAndGet();
                if (denied.get()) {
                    throw new SecurityException("fixture KMS access revoked");
                }
                return key.duplicate();
            }
        });
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
    public void commit(TableMetadata base, TableMetadata updated) {
        metadata = updated;
    }

    @Override
    public FileIO io() {
        return writerIo;
    }

    @Override
    public EncryptionManager encryption() {
        return writer;
    }

    @Override
    public String metadataFileLocation(String name) {
        return metadata.location() + "/metadata/" + name;
    }

    @Override
    public LocationProvider locationProvider() {
        return LocationProviders.locationsFor(metadata.location(), metadata.properties());
    }

    @Override
    public void close() {
        raw.close();
    }
}
