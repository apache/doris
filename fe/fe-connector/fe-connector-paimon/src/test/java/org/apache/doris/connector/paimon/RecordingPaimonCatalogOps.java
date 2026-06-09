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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Hand-written recording fake for {@link PaimonCatalogOps} (no Mockito), mirroring the
 * maxcompute connector's recording {@code McStructureHelper}.
 *
 * <p>Records an ordered call log, returns configurable fixed data, and can be told to throw
 * the paimon {@code DatabaseNotExistException} / {@code TableNotExistException} (and the B3
 * DDL exceptions) that the production code catches/wraps. Because the seam fully covers every
 * remote call {@link PaimonConnectorMetadata} makes, the metadata under test is built with a
 * {@code null} real Catalog — the test stays entirely offline.
 */
final class RecordingPaimonCatalogOps implements PaimonCatalogOps {

    final List<String> log = new ArrayList<>();

    List<String> databases = new ArrayList<>();
    List<String> tables = new ArrayList<>();
    Table table;
    List<Partition> partitions = new ArrayList<>();

    boolean throwDatabaseNotExist;
    boolean throwTableNotExist;

    // ---- B3 DDL capture fields (inputs the metadata layer passed to the seam) ----
    Schema lastCreatedSchema;
    Identifier lastCreatedTableId;
    boolean lastCreateTableIgnoreIfExists;
    Identifier lastDroppedTableId;
    boolean lastDropTableIgnoreIfNotExists;
    String lastCreatedDb;
    Map<String, String> lastCreatedDbProps;
    boolean lastCreateDbIgnoreIfExists;
    String lastDroppedDb;
    boolean lastDropCascade;
    boolean lastDropDbIgnoreIfNotExists;

    // ---- B3 DDL throw flags (mirror the read-path throwDatabaseNotExist/throwTableNotExist) ----
    boolean throwTableAlreadyExist;
    boolean throwTableNotExistOnDrop;
    boolean throwDatabaseAlreadyExist;
    boolean throwDatabaseNotEmpty;
    boolean throwDatabaseNotExistOnDrop;

    @Override
    public List<String> listDatabases() {
        log.add("listDatabases");
        return databases;
    }

    @Override
    public Database getDatabase(String name) throws Catalog.DatabaseNotExistException {
        log.add("getDatabase:" + name);
        if (throwDatabaseNotExist) {
            throw new Catalog.DatabaseNotExistException(name);
        }
        // databaseExists ignores the returned Database (only the throw/no-throw matters),
        // so a null is sufficient and keeps the fake free of a Database double.
        return null;
    }

    @Override
    public List<String> listTables(String databaseName) throws Catalog.DatabaseNotExistException {
        log.add("listTables:" + databaseName);
        if (throwDatabaseNotExist) {
            throw new Catalog.DatabaseNotExistException(databaseName);
        }
        return tables;
    }

    @Override
    public Table getTable(Identifier identifier) throws Catalog.TableNotExistException {
        log.add("getTable:" + identifier.getFullName());
        if (throwTableNotExist) {
            throw new Catalog.TableNotExistException(identifier);
        }
        return table;
    }

    @Override
    public List<Partition> listPartitions(Identifier identifier) throws Catalog.TableNotExistException {
        log.add("listPartitions:" + identifier.getFullName());
        if (throwTableNotExist) {
            throw new Catalog.TableNotExistException(identifier);
        }
        return partitions;
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
            throws Catalog.DatabaseAlreadyExistException {
        log.add("createDatabase:" + name);
        lastCreatedDb = name;
        lastCreateDbIgnoreIfExists = ignoreIfExists;
        lastCreatedDbProps = properties;
        if (throwDatabaseAlreadyExist) {
            throw new Catalog.DatabaseAlreadyExistException(name);
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws Catalog.DatabaseNotExistException, Catalog.DatabaseNotEmptyException {
        log.add("dropDatabase:" + name + ",cascade=" + cascade);
        lastDroppedDb = name;
        lastDropDbIgnoreIfNotExists = ignoreIfNotExists;
        lastDropCascade = cascade;
        if (throwDatabaseNotExistOnDrop) {
            throw new Catalog.DatabaseNotExistException(name);
        }
        if (throwDatabaseNotEmpty) {
            throw new Catalog.DatabaseNotEmptyException(name);
        }
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws Catalog.TableAlreadyExistException, Catalog.DatabaseNotExistException {
        log.add("createTable:" + identifier.getFullName());
        lastCreatedTableId = identifier;
        lastCreatedSchema = schema;
        lastCreateTableIgnoreIfExists = ignoreIfExists;
        if (throwTableAlreadyExist) {
            throw new Catalog.TableAlreadyExistException(identifier);
        }
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws Catalog.TableNotExistException {
        log.add("dropTable:" + identifier.getFullName());
        lastDroppedTableId = identifier;
        lastDropTableIgnoreIfNotExists = ignoreIfNotExists;
        if (throwTableNotExistOnDrop || throwTableNotExist) {
            throw new Catalog.TableNotExistException(identifier);
        }
    }

    @Override
    public void close() {
        log.add("close");
    }
}
