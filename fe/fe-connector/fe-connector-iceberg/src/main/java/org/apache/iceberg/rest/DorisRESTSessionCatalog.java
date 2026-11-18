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

package org.apache.iceberg.rest;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;

import org.apache.doris.connector.iceberg.IcebergRestEncryptionFileIO;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Adds encrypted metadata reads to the pinned SDK's REST operations. This package is required because the
 * protected SDK factory returns package-private RESTTableOperations; RESTTable and its scan capabilities stay intact.
 */
public final class DorisRESTSessionCatalog extends RESTSessionCatalog {
    private final Function<RESTSessionCatalog, FileIO> catalogFileIO;

    public DorisRESTSessionCatalog(Function<Map<String, String>, RESTClient> clientBuilder,
            Function<RESTSessionCatalog, FileIO> catalogFileIO) {
        super(clientBuilder, null);
        this.catalogFileIO = catalogFileIO;
    }

    @Override
    protected RESTTableOperations newTableOps(RESTClient client, String path,
            Supplier<Map<String, String>> readHeaders, Supplier<Map<String, String>> mutationHeaders,
            FileIO io, TableMetadata metadata, Set<Endpoint> endpoints, Map<String, String> readQueryParams) {
        if (!metadata.properties().containsKey(TableProperties.ENCRYPTION_TABLE_KEY)
                && metadata.encryptionKeys().isEmpty()) {
            return super.newTableOps(client, path, readHeaders, mutationHeaders, io, metadata,
                    endpoints, readQueryParams);
        }
        FileIO shared = Objects.requireNonNull(catalogFileIO.apply(this), "REST catalog FileIO ownership");
        return new EncryptedTableOperations(client, path, readHeaders, mutationHeaders, io, metadata,
                endpoints, readQueryParams, io != shared);
    }

    private static final class EncryptedTableOperations extends RESTTableOperations {
        private final AtomicReference<TableMetadata> readMetadata;
        private final IcebergRestEncryptionFileIO encryptedIO;

        private EncryptedTableOperations(RESTClient client, String path,
                Supplier<Map<String, String>> readHeaders, Supplier<Map<String, String>> mutationHeaders,
                FileIO io, TableMetadata metadata, Set<Endpoint> endpoints, Map<String, String> readQueryParams,
                boolean ownsIO) {
            super(client, path, readHeaders, mutationHeaders, io, metadata, endpoints, readQueryParams);
            // The SDK's FileIOTracker has weak operation keys and strong FileIO values. Sharing only a metadata
            // holder, rather than passing this::current into the FileIO, keeps that tracker from retaining the key.
            readMetadata = new AtomicReference<>(metadata);
            encryptedIO = new IcebergRestEncryptionFileIO(io, readMetadata, ownsIO);
        }

        @Override
        public TableMetadata refresh() {
            TableMetadata refreshed = super.refresh();
            readMetadata.set(refreshed);
            return refreshed;
        }

        @Override
        public FileIO io() {
            return encryptedIO;
        }

        @Override
        public EncryptionManager encryption() {
            return encryptedIO.encryptionManager();
        }

        @Override
        public TableOperations temp(TableMetadata uncommittedMetadata) {
            // Enforce read-only semantics at transaction creation, before a caller can stage
            // mutable metadata or initialize the table's KMS client for a write.
            throw new UnsupportedOperationException("Encrypted Iceberg REST tables are read-only in Doris");
        }

        @Override
        public void commit(TableMetadata base, TableMetadata updated) {
            throw new UnsupportedOperationException("Encrypted Iceberg REST tables are read-only in Doris");
        }
    }
}
