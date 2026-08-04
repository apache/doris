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

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

import java.util.Objects;

/** Read-only operations that retain one immutable Iceberg metadata generation. */
final class IcebergSnapshotTableOperations implements TableOperations {

    private final TableOperations delegate;
    private final TableMetadata metadata;

    IcebergSnapshotTableOperations(TableOperations delegate, TableMetadata metadata) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
    }

    @Override
    public TableMetadata current() {
        return metadata;
    }

    @Override
    public TableMetadata refresh() {
        // A statement's bound slots and later metadata rows must keep the same generation even if
        // a writable BaseTable sharing the catalog cache refreshes its own operations concurrently.
        return metadata;
    }

    @Override
    public void commit(TableMetadata base, TableMetadata updated) {
        throw new UnsupportedOperationException("Statement snapshot table is read-only");
    }

    @Override
    public FileIO io() {
        return delegate.io();
    }

    @Override
    public EncryptionManager encryption() {
        return delegate.encryption();
    }

    @Override
    public String metadataFileLocation(String fileName) {
        return delegate.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() {
        return delegate.locationProvider();
    }
}
