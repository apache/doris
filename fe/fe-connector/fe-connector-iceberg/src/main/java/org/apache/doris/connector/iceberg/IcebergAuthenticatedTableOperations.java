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

/**
 * A {@link TableOperations} decorator that forwards every call to the delegate EXCEPT {@link #io()}, which returns
 * an auth-wrapping {@link IcebergAuthenticatedFileIO}. Used by {@code IcebergConnectorTransaction.openTransaction}
 * to build a Kerberos transaction via {@code Transactions.newTransaction(name, ops, reporter)}: iceberg's
 * {@code SnapshotProducer} takes its manifest {@code OutputFile} from {@code ops.io()}, so routing {@code io()}
 * through the wrapper is what carries the plugin Kerberos {@code doAs} onto the worker-pool manifest writes.
 *
 * <p>Only {@code io()} is altered; {@code current}/{@code refresh}/{@code commit} and the metadata/location seams
 * forward unchanged, so commit semantics (optimistic concurrency, metadata JSON write on the caller thread — which
 * is already inside the caller-thread {@code doAs}) are byte-for-byte the delegate's. {@code temp()} must ALSO
 * wrap: {@code BaseTransaction.TransactionTableOperations} never reads {@code io()} from this instance — its
 * {@code io()} returns {@code tempOps.io()} where {@code tempOps = ops.temp(current)} (rebuilt on every
 * intermediate commit), and that is exactly where {@code SnapshotProducer} takes the manifest {@code OutputFile}
 * from. Forwarding {@code temp()} unwrapped hands the raw FileIO to the worker-pool manifest writes and reopens
 * the Kerberos SIMPLE-auth failure this class exists to fix.
 */
final class IcebergAuthenticatedTableOperations implements TableOperations {

    private final TableOperations delegate;
    private final FileIO io;

    IcebergAuthenticatedTableOperations(TableOperations delegate, FileIO io) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.io = Objects.requireNonNull(io, "io");
    }

    @Override
    public TableMetadata current() {
        return delegate.current();
    }

    @Override
    public TableMetadata refresh() {
        return delegate.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
        delegate.commit(base, metadata);
    }

    @Override
    public FileIO io() {
        return io;
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

    @Override
    public TableOperations temp(TableMetadata uncommittedMetadata) {
        // The delegate's temp ops (e.g. HadoopTableOperations.temp) expose the RAW FileIO; re-wrap so the
        // transaction's tempOps — the io() source for worker-pool manifest writes — stays authenticated.
        return new IcebergAuthenticatedTableOperations(delegate.temp(uncommittedMetadata), io);
    }

    @Override
    public long newSnapshotId() {
        return delegate.newSnapshotId();
    }

    @Override
    public boolean requireStrictCleanup() {
        return delegate.requireStrictCleanup();
    }
}
