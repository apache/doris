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

import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.encryption.StandardEncryptionManager;

/** Shared admission contract; encrypted ranges remain forbidden until the engine can prove BE support. */
final class IcebergEncryptionRead {
    private IcebergEncryptionRead() {
    }

    static void check(Table table, IcebergCatalogProperties catalog, ConnectorContext context) {
        TableMetadata metadata = table instanceof HasTableOperations
                ? ((HasTableOperations) table).operations().current() : null;
        // Removing the current write key does not make old snapshots plaintext. Retained table keys must
        // fence metadata-only COUNT and system tables before either path can bypass native reader admission.
        boolean declared = table.properties().containsKey(TableProperties.ENCRYPTION_TABLE_KEY)
                || (metadata != null && !metadata.encryptionKeys().isEmpty());
        EncryptionManager manager = declared ? null : table.encryption();
        if (!declared && (manager == null || manager instanceof PlaintextEncryptionManager)) {
            return;
        }
        if (!IcebergConf.encryptionReadEnabled(context)) {
            throw failure("READ_DISABLED", "Encrypted Iceberg reads are disabled; enable_encryption_read in "
                    + "iceberg.conf defaults to false");
        }
        if (!IcebergCatalogProperties.TYPE_HMS.equals(catalog.getFlavor())) {
            throw failure("UNSUPPORTED_CATALOG", "Encrypted Iceberg reads require an HMS catalog");
        }
        if (metadata == null || metadata.formatVersion() != 3) {
            throw failure("UNSUPPORTED_TABLE_FORMAT", "Encrypted Iceberg reads require table format v3");
        }
        // A declared table needs no KMS initialization just to reject an unverified execution path.
        // Custom managers observed during detection can already be classified without resolving any keys.
        if (manager != null && !(manager instanceof StandardEncryptionManager)) {
            throw failure("UNSUPPORTED_ENCRYPTION_MANAGER", "Encrypted Iceberg reads require the SDK standard "
                    + "encryption scheme with native Parquet AES_GCM_V1 files");
        }
        // A PME-enabled Arrow build does not prove key-transport, delete-reader or mixed-version scheduling
        // support. Until explicit BE negotiation is wired, even an enabled FE must never emit encrypted tasks.
        throw failure("BACKEND_CAPABILITY_UNAVAILABLE", "Encrypted Iceberg read capability is not verified "
                + "by the engine; native encrypted scan execution is not available");
    }

    private static DorisConnectorException failure(String classification, String message) {
        // Never attach table properties, key identifiers or SDK encryption objects to this error.
        return new DorisConnectorException("Iceberg encryption [" + classification + "]: " + message);
    }
}
