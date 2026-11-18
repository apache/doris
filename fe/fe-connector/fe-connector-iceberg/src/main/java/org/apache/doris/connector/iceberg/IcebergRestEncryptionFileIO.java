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

import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestListFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/** Read-only REST file adapter; its KMS client has the same lifetime as the loaded table's FileIO. */
public final class IcebergRestEncryptionFileIO implements FileIO {
    private final FileIO delegate;
    private final AtomicReference<TableMetadata> metadata;
    private final boolean ownsDelegate;
    private transient KeyManagementClient kms;
    private transient TableMetadata encryptionMetadata;
    private transient EncryptingFileIO encrypted;
    private boolean closed;

    public IcebergRestEncryptionFileIO(FileIO delegate, AtomicReference<TableMetadata> metadata,
            boolean ownsDelegate) {
        this.delegate = delegate;
        this.metadata = metadata;
        this.ownsDelegate = ownsDelegate;
    }

    public EncryptionManager encryptionManager() {
        return encrypted().encryptionManager();
    }

    private synchronized EncryptingFileIO encrypted() {
        if (closed) {
            throw new IllegalStateException("Iceberg REST table FileIO is closed");
        }
        TableMetadata current = metadata.get();
        if (encrypted == null || encryptionMetadata != current) {
            // Retained keys still describe encrypted snapshots. The SDK factory selects plaintext when the
            // current write-key property is missing, so reject that invalid catalog state before any KMS work.
            if (!current.properties().containsKey(TableProperties.ENCRYPTION_TABLE_KEY)) {
                throw new DorisConnectorException("Iceberg encryption [MISSING_TABLE_KEY]");
            }
            Map<String, String> properties = delegate.properties();
            // The SDK's reflective KMS factory otherwise fails with a null implementation class name.
            if (properties.get(CatalogProperties.ENCRYPTION_KMS_TYPE) == null
                    && properties.get(CatalogProperties.ENCRYPTION_KMS_IMPL) == null) {
                throw new DorisConnectorException("Iceberg encryption [MISSING_KMS]");
            }
            // REST load/config may vend table-specific properties. Do not replace them with catalog credentials,
            // and do not initialize a KMS merely to reject an encrypted table at the read admission boundary.
            if (kms == null) {
                kms = EncryptionUtil.createKmsClient(properties);
            }
            EncryptionManager manager = EncryptionUtil.createEncryptionManager(
                    current.encryptionKeys(), current.properties(), kms);
            encrypted = EncryptingFileIO.combine(delegate, manager);
            encryptionMetadata = current;
        }
        return encrypted;
    }

    @Override
    public InputFile newInputFile(String path) {
        return delegate.newInputFile(path);
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        return delegate.newInputFile(path, length);
    }

    @Override
    public InputFile newInputFile(DataFile file) {
        return file.keyMetadata() == null ? delegate.newInputFile(file) : encrypted().newInputFile(file);
    }

    @Override
    public InputFile newInputFile(DeleteFile file) {
        return file.keyMetadata() == null ? delegate.newInputFile(file) : encrypted().newInputFile(file);
    }

    @Override
    public InputFile newInputFile(ManifestFile file) {
        return file.keyMetadata() == null ? delegate.newInputFile(file) : encrypted().newInputFile(file);
    }

    @Override
    public InputFile newInputFile(ManifestListFile file) {
        return file.encryptionKeyID() == null ? delegate.newInputFile(file) : encrypted().newInputFile(file);
    }

    @Override
    public OutputFile newOutputFile(String path) {
        throw new UnsupportedOperationException("Encrypted Iceberg REST tables are read-only in Doris");
    }

    @Override
    public void deleteFile(String path) {
        throw new UnsupportedOperationException("Encrypted Iceberg REST tables are read-only in Doris");
    }

    @Override
    public Map<String, String> properties() {
        return delegate.properties();
    }

    FileIO delegate() {
        return delegate;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        // The SDK also tracks this adapter. A table lease and catalog retirement may both close it;
        // neither may close a shared catalog FileIO, and an independently owned delegate must still retire.
        try (KeyManagementClient ownedKms = kms) {
            if (ownsDelegate) {
                delegate.close();
            }
        } finally {
            encrypted = null;
            encryptionMetadata = null;
            kms = null;
        }
    }
}
