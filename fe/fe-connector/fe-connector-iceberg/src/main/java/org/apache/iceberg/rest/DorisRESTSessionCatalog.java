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
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;

import org.apache.doris.connector.iceberg.IcebergRestEncryptionFileIO;
import org.apache.doris.connector.spi.DorisConnectorException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Adds encrypted metadata reads and schema-checked property commits to the pinned Iceberg SDK. The
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
            return new SchemaFencedTableOperations(client, path, readHeaders, mutationHeaders, io, metadata,
                    endpoints, readQueryParams);
        }
        FileIO shared = Objects.requireNonNull(catalogFileIO.apply(this), "REST catalog FileIO ownership");
        return new EncryptedTableOperations(client, path, readHeaders, mutationHeaders, io, metadata,
                endpoints, readQueryParams, io != shared);
    }

    public static boolean isRestTableOperations(TableOperations operations) {
        return operations instanceof RESTTableOperations;
    }

    public static void commitWithCurrentSchema(TableOperations operations, TableMetadata base, TableMetadata updated) {
        if (!(operations instanceof SchemaFencedTableOperations)) {
            throw new DorisConnectorException("Cannot safely upgrade an Iceberg REST table without a schema check");
        }
        ((SchemaFencedTableOperations) operations).commitWithCurrentSchema(base, updated);
    }

    private static class SchemaFencedTableOperations extends RESTTableOperations {
        private final RESTClient client;
        private final String path;
        private final Supplier<Map<String, String>> mutationHeaders;
        private final Set<Endpoint> endpoints;

        private SchemaFencedTableOperations(RESTClient client, String path,
                Supplier<Map<String, String>> readHeaders, Supplier<Map<String, String>> mutationHeaders,
                FileIO io, TableMetadata metadata, Set<Endpoint> endpoints, Map<String, String> readQueryParams) {
            super(client, path, readHeaders, mutationHeaders, io, metadata, endpoints, readQueryParams);
            this.client = client;
            this.path = path;
            this.mutationHeaders = mutationHeaders;
            this.endpoints = endpoints;
        }

        void commitWithCurrentSchema(TableMetadata base, TableMetadata updated) {
            Endpoint.check(endpoints, Endpoint.V1_UPDATE_TABLE);
            List<UpdateRequirement> requirements = new ArrayList<>(
                    UpdateRequirements.forUpdateTable(base, updated.changes()));
            requirements.add(new UpdateRequirement.AssertCurrentSchemaID(base.currentSchemaId()));
            UpdateTableRequest request = new UpdateTableRequest(requirements, updated.changes());
            // The caller discards these operations after DDL. Use the table client's existing authentication
            // and Iceberg's conflict/unknown-commit-state classification.
            client.post(path, request, LoadTableResponse.class, mutationHeaders, ErrorHandlers.tableCommitHandler());
        }
    }

    private static final class EncryptedTableOperations extends SchemaFencedTableOperations {
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
        void commitWithCurrentSchema(TableMetadata base, TableMetadata updated) {
            commit(base, updated);
        }

        @Override
        public void commit(TableMetadata base, TableMetadata updated) {
            throw new UnsupportedOperationException("Encrypted Iceberg REST tables are read-only in Doris");
        }
    }
}
