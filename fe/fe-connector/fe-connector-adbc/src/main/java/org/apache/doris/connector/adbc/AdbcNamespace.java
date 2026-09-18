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

import org.apache.doris.connector.spi.DorisConnectorException;

import java.util.Objects;

/**
 * One remote {@code (catalog, db_schema)} pair, and the Doris database name it presents as.
 *
 * <p>ADBC names objects in three levels (catalog / db_schema / table) while a Doris external table has two
 * (database / table), because the outermost name is already spent on the catalog a user created. Something
 * has to give, and this is where.
 *
 * <p><b>The Doris database name is a display and lookup key only; it is never parsed back.</b> A catalog
 * name may legally contain a dot, so a name built by joining two remote levels could not be split again
 * unambiguously -- which is why {@link AdbcTableHandle} carries the three remote parts separately instead of
 * re-deriving them. The {@code uri} property is required to pin the remote catalog, which is what keeps the
 * mapping a projection rather than a join: at most one of the two remote levels varies.
 */
public final class AdbcNamespace {

    private final String remoteCatalog;
    private final String remoteDbSchema;

    public AdbcNamespace(String remoteCatalog, String remoteDbSchema) {
        // Empty and null both mean "this source has no such level" -- SQLite reports db_schema as the empty
        // string, other drivers report null, and the difference is not meaningful.
        this.remoteCatalog = remoteCatalog == null ? "" : remoteCatalog;
        this.remoteDbSchema = remoteDbSchema == null ? "" : remoteDbSchema;
    }

    public String getRemoteCatalog() {
        return remoteCatalog;
    }

    public String getRemoteDbSchema() {
        return remoteDbSchema;
    }

    /**
     * The name this namespace shows up as in {@code SHOW DATABASES}.
     *
     * <p>When both levels are populated the schema wins: the catalog is pinned by {@code uri}, so it is the
     * same for every namespace in this Doris catalog and would add nothing but ambiguity to the name.
     */
    public String dorisDatabaseName() {
        if (!remoteDbSchema.isEmpty()) {
            return remoteDbSchema;
        }
        if (!remoteCatalog.isEmpty()) {
            return remoteCatalog;
        }
        throw new DorisConnectorException(
                "The ADBC source reported an object with neither a catalog nor a database schema name,"
                        + " so it cannot be addressed as a Doris database");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AdbcNamespace)) {
            return false;
        }
        AdbcNamespace other = (AdbcNamespace) o;
        return remoteCatalog.equals(other.remoteCatalog) && remoteDbSchema.equals(other.remoteDbSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(remoteCatalog, remoteDbSchema);
    }

    @Override
    public String toString() {
        return "AdbcNamespace{catalog='" + remoteCatalog + "', dbSchema='" + remoteDbSchema + "'}";
    }
}
