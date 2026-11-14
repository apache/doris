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

import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.ResolvingFileIO;
import org.apache.paimon.fs.hadoop.HadoopFileIO;
import org.apache.paimon.hive.HiveCatalogLockFactory;
import org.apache.paimon.jdbc.JdbcCatalogLockFactory;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;

import java.io.IOException;

/** Admission before creating a BE sink; this does not implement a distributed table lock. */
final class PaimonWriteSafety {
    private static final Logger LOG = LogManager.getLogger(PaimonWriteSafety.class);

    private PaimonWriteSafety() {
    }

    static void validate(FileStoreTable table, PaimonCatalogProperties properties) {
        String reason = rejectionReason(table);
        if (reason == null) {
            return;
        }
        if (properties.isAllowUnsafeWrites()) {
            LOG.warn("Allow unprotected Paimon write to {}: {}. All writers, including compaction, "
                    + "must be externally serialized ({}=true).", table.name(), reason,
                    PaimonCatalogProperties.ALLOW_UNSAFE_WRITES);
            return;
        }
        throw new DorisConnectorException("Cannot write to Paimon table " + table.name() + ": " + reason
                + ". Use catalog-managed snapshot commits, HDFS, or an enabled Hive catalog lock. "
                + "Set '" + PaimonCatalogProperties.ALLOW_UNSAFE_WRITES + "'='true' on the catalog only "
                + "when all writers to this table, including Spark, Flink and compaction jobs, "
                + "are externally serialized");
    }

    // Use the environment the SDK actually commits through, rather than paimon.catalog.type or
    // lock.enabled: a filesystem catalog can have lock.enabled=true and still have no lock factory.
    static String rejectionReason(FileStoreTable table) {
        CatalogEnvironment environment = table.catalogEnvironment();
        if (environment.catalogLoader() != null && environment.supportsVersionManagement()) {
            return null;
        }
        FileIO fileIO = table.fileIO();
        try {
            if (fileIO instanceof ResolvingFileIO) {
                fileIO = ((ResolvingFileIO) fileIO).fileIO(table.location());
            }
        } catch (IOException e) {
            throw new DorisConnectorException("Cannot resolve snapshot filesystem for Paimon table "
                    + table.name(), e);
        }
        // LocalFileIO also reports isObjectStore=false, but its rename lock only protects one JVM.
        // Do not extend this to viewfs: its mounts can point to object storage.
        if ("hdfs".equalsIgnoreCase(table.location().toUri().getScheme())
                && fileIO.getClass() == HadoopFileIO.class) {
            return null;
        }
        CatalogLockFactory lockFactory = environment.lockFactory();
        if (lockFactory instanceof HiveCatalogLockFactory) {
            return null;
        }
        if (lockFactory instanceof JdbcCatalogLockFactory) {
            // Paimon 2.1 leases have no owner token: expiry does not fence the previous writer.
            return "the Paimon JDBC lease lock does not fence an expired lock holder";
        }
        return "snapshot publication has no supported concurrency protection (filesystem="
                + table.location().toUri().getScheme() + ", lock="
                + (lockFactory == null ? "none" : lockFactory.identifier()) + ")";
    }
}
