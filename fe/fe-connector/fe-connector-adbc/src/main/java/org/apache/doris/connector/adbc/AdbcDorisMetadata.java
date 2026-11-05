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

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/** Metadata confined to the remote catalog that the ADBC connection uses for SQL execution. */
final class AdbcDorisMetadata {
    private static final DorisDialect DIALECT = new DorisDialect();

    private AdbcDorisMetadata() {
    }

    static boolean isDorisSource(AdbcClient client) {
        // sql_dialect changes SQL rendering, not the remote vendor's metadata capabilities.
        return client.vendorName().map(String::trim).filter(DIALECT::matchesVendor).isPresent();
    }

    static List<AdbcNamespace> readNamespaces(AdbcConnection connection) throws AdbcException, IOException {
        List<String> catalogs = new ArrayList<>(1);
        // Older Doris servers expose the current catalog through SQL but lack Flight GetSessionOptions.
        readRows(connection, "SELECT CURRENT_CATALOG()", row -> catalogs.add(row.get(0)));
        String catalog = catalogs.get(0);
        if (catalog == null || catalog.isEmpty()) {
            throw new DorisConnectorException("The Doris ADBC connection must select a remote catalog");
        }
        List<AdbcNamespace> namespaces = new ArrayList<>();
        // The Flight driver omits catalog filters in GetObjects RPCs. On a loopback source, unrestricted
        // discovery reenters the ADBC catalog whose metadata load is waiting for that RPC to complete.
        readRows(connection, "SHOW DATABASES FROM " + DIALECT.quoteIdentifier(catalog),
                row -> namespaces.add(new AdbcNamespace(catalog, row.get(0))));
        return namespaces;
    }

    static List<String> readTableNames(AdbcConnection connection, AdbcNamespace namespace)
            throws AdbcException, IOException {
        List<String> tables = new ArrayList<>();
        // Scope the actual SQL request, not just GetObjects' client-side result filtering.
        readRows(connection, "SHOW FULL TABLES FROM " + DIALECT.quoteIdentifier(namespace.getRemoteCatalog())
                + "." + DIALECT.quoteIdentifier(namespace.getRemoteDbSchema()), row -> {
                    if (AdbcObjectsReader.isBaseTable(row.get(1))) {
                        tables.add(row.get(0));
                    }
                });
        return tables;
    }

    private static void readRows(AdbcConnection connection, String sql, Consumer<List<String>> consumer)
            throws AdbcException, IOException {
        try (AdbcStatement statement = connection.createStatement()) {
            statement.setSqlQuery(sql);
            try (AdbcStatement.QueryResult result = statement.executeQuery()) {
                ArrowReader reader = result.getReader();
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                while (reader.loadNextBatch()) {
                    for (int row = 0; row < root.getRowCount(); row++) {
                        List<String> values = new ArrayList<>();
                        for (int column = 0; column < root.getFieldVectors().size(); column++) {
                            values.add(root.getVector(column).getObject(row).toString());
                        }
                        consumer.accept(values);
                    }
                }
            }
        }
    }
}
