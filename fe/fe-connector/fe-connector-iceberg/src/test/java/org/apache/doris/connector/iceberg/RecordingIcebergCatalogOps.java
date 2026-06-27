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

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Hand-written recording fake for {@link IcebergCatalogOps} (no Mockito), mirroring the paimon
 * connector's {@code RecordingPaimonCatalogOps}.
 *
 * <p>Records an ordered call log, returns configurable fixed data, and can be told that a table
 * does not exist ({@link #tableExists} returns the canned {@link #tableExists} boolean) or that
 * {@link #loadTable} should fail (via {@link #throwOnLoadTable}). Because the seam fully covers
 * every remote call {@link IcebergConnectorMetadata} makes, the metadata under test is built with
 * a {@code null} real Catalog — the test stays entirely offline.
 */
final class RecordingIcebergCatalogOps implements IcebergCatalogOps {

    final List<String> log = new ArrayList<>();

    /** Canned database (namespace) names returned by {@link #listDatabaseNames()}. */
    List<String> databases = new ArrayList<>();
    /** Canned table names returned by {@link #listTableNames(String)}. */
    List<String> tables = new ArrayList<>();
    /** Canned existence answer for {@link #databaseExists(String)}. */
    boolean databaseExists;
    /** Canned existence answer for {@link #tableExists(String, String)}. */
    boolean tableExists;
    /** Canned table returned by {@link #loadTable(String, String)}. */
    Table table;
    /** When set, {@link #loadTable(String, String)} throws instead of returning {@link #table}. */
    boolean throwOnLoadTable;

    /** The (dbName, tableName) the metadata layer passed to the most recent {@link #loadTable}. */
    String lastLoadDb;
    String lastLoadTable;
    /** The (dbName, tableName) the metadata layer passed to the most recent {@link #tableExists}. */
    String lastExistsDb;
    String lastExistsTable;

    // ---- DDL write recording (B1) ----
    String lastCreateDb;
    Map<String, String> lastCreateDbProps;
    String lastDropDb;
    String lastCreateTableDb;
    String lastCreateTableName;
    Schema lastCreateSchema;
    PartitionSpec lastCreateSpec;
    SortOrder lastCreateSortOrder;
    Map<String, String> lastCreateProps;
    String lastDropTableDb;
    String lastDropTableName;
    boolean lastDropPurge;
    /** Canned location answers for the load-before-drop helpers (default: absent). */
    Optional<String> tableLocation = Optional.empty();
    Optional<String> namespaceLocation = Optional.empty();

    @Override
    public List<String> listDatabaseNames() {
        log.add("listDatabaseNames");
        return databases;
    }

    @Override
    public boolean databaseExists(String dbName) {
        log.add("databaseExists:" + dbName);
        return databaseExists;
    }

    @Override
    public List<String> listTableNames(String dbName) {
        log.add("listTableNames:" + dbName);
        return tables;
    }

    @Override
    public boolean tableExists(String dbName, String tableName) {
        log.add("tableExists:" + dbName + "." + tableName);
        lastExistsDb = dbName;
        lastExistsTable = tableName;
        return tableExists;
    }

    @Override
    public Table loadTable(String dbName, String tableName) {
        log.add("loadTable:" + dbName + "." + tableName);
        lastLoadDb = dbName;
        lastLoadTable = tableName;
        if (throwOnLoadTable) {
            throw new RuntimeException("simulated loadTable failure for " + dbName + "." + tableName);
        }
        return table;
    }

    @Override
    public void createDatabase(String dbName, Map<String, String> properties) {
        log.add("createDatabase:" + dbName);
        lastCreateDb = dbName;
        lastCreateDbProps = properties;
    }

    @Override
    public void dropDatabase(String dbName) {
        log.add("dropDatabase:" + dbName);
        lastDropDb = dbName;
    }

    @Override
    public void createTable(String dbName, String tableName, Schema schema, PartitionSpec partitionSpec,
            SortOrder sortOrder, Map<String, String> properties) {
        log.add("createTable:" + dbName + "." + tableName);
        lastCreateTableDb = dbName;
        lastCreateTableName = tableName;
        lastCreateSchema = schema;
        lastCreateSpec = partitionSpec;
        lastCreateSortOrder = sortOrder;
        lastCreateProps = properties;
    }

    @Override
    public void dropTable(String dbName, String tableName, boolean purge) {
        log.add("dropTable:" + dbName + "." + tableName + ":purge=" + purge);
        lastDropTableDb = dbName;
        lastDropTableName = tableName;
        lastDropPurge = purge;
    }

    @Override
    public Optional<String> loadTableLocation(String dbName, String tableName) {
        log.add("loadTableLocation:" + dbName + "." + tableName);
        return tableLocation;
    }

    @Override
    public Optional<String> loadNamespaceLocation(String dbName) {
        log.add("loadNamespaceLocation:" + dbName);
        return namespaceLocation;
    }

    @Override
    public void close() {
        log.add("close");
    }
}
