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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

/**
 * Table handle carrying the remote coordinates of one ADBC table.
 *
 * <p><b>All three remote levels are stored separately, and none is ever re-derived from the Doris name.</b>
 * Doris shows a two-level name, so a handle that kept only that would have to split it back apart to
 * address the table remotely -- and that split has no correct implementation: a remote catalog or schema
 * name may itself contain a dot, so {@code MY.DB.PUBLIC} has several readings and the wrong one addresses a
 * table that does not exist. Keeping the parts is what makes the question never arise.
 */
public class AdbcTableHandle implements ConnectorTableHandle {

    private static final long serialVersionUID = 1L;

    private final String remoteCatalog;
    private final String remoteDbSchema;
    private final String remoteTable;
    private final String dorisDbName;

    public AdbcTableHandle(AdbcNamespace namespace, String remoteTable) {
        this.remoteCatalog = namespace.getRemoteCatalog();
        this.remoteDbSchema = namespace.getRemoteDbSchema();
        this.remoteTable = remoteTable;
        this.dorisDbName = namespace.dorisDatabaseName();
    }

    public String getRemoteCatalog() {
        return remoteCatalog;
    }

    public String getRemoteDbSchema() {
        return remoteDbSchema;
    }

    public String getRemoteTable() {
        return remoteTable;
    }

    /** Display and lookup key only. Never parsed. */
    public String getDorisDbName() {
        return dorisDbName;
    }

    public AdbcNamespace getNamespace() {
        return new AdbcNamespace(remoteCatalog, remoteDbSchema);
    }

    @Override
    public String toString() {
        return "AdbcTableHandle{catalog='" + remoteCatalog + "', dbSchema='" + remoteDbSchema
                + "', table='" + remoteTable + "'}";
    }
}
