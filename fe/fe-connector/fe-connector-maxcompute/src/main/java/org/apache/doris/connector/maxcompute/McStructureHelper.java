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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.DorisConnectorException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.Project;
import com.aliyun.odps.Schema;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.Tables;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Abstraction for MaxCompute structure mapping between Doris and MC.
 * Handles two modes based on mc.enable.namespace.schema property:
 * <ul>
 *   <li>true: project → schema → table (namespace schema mode)</li>
 *   <li>false: project → table (legacy mode)</li>
 * </ul>
 * Adapted from fe-core McStructureHelper with DdlException replaced
 * by DorisConnectorException.
 */
public interface McStructureHelper {
    List<String> listTableNames(Odps mcClient, String dbName);

    List<String> listDatabaseNames(Odps mcClient, String defaultProject);

    boolean tableExist(Odps mcClient, String dbName, String tableName);

    boolean databaseExist(Odps mcClient, String dbName);

    TableIdentifier getTableIdentifier(String dbName, String tableName);

    List<Partition> getPartitions(
            Odps mcClient, String dbName, String tableName);

    Iterator<Partition> getPartitionIterator(
            Odps mcClient, String dbName, String tableName);

    Table getOdpsTable(Odps mcClient, String dbName, String tableName);

    Tables.TableCreator createTableCreator(
            Odps mcClient, String dbName,
            String tableName, TableSchema schema);

    void dropTable(Odps mcClient, String dbName,
            String tableName, boolean ifExists) throws OdpsException;

    void createDb(Odps mcClient, String dbName, boolean ifNotExists);

    void dropDb(Odps mcClient, String dbName, boolean ifExists);

    /**
     * mc.enable.namespace.schema = true.
     * Mapping: Doris(catalog, db, table) → MC(project, schema, table).
     */
    class ProjectSchemaTableHelper implements McStructureHelper {
        private final String defaultProjectName;

        ProjectSchemaTableHelper(String defaultProjectName) {
            this.defaultProjectName = defaultProjectName;
        }

        @Override
        public List<String> listTableNames(Odps mcClient, String dbName) {
            List<String> result = new ArrayList<>();
            mcClient.tables()
                    .iterable(defaultProjectName, dbName, null, false)
                    .forEach(e -> result.add(e.getName()));
            return result;
        }

        @Override
        public List<String> listDatabaseNames(
                Odps mcClient, String defaultProject) {
            List<String> result = new ArrayList<>();
            Iterator<Schema> iterator =
                    mcClient.schemas().iterator(defaultProjectName);
            while (iterator.hasNext()) {
                result.add(iterator.next().getName());
            }
            return result;
        }

        @Override
        public List<Partition> getPartitions(
                Odps mcClient, String dbName, String tableName) {
            return mcClient.tables()
                    .get(defaultProjectName, dbName, tableName)
                    .getPartitions();
        }

        @Override
        public Iterator<Partition> getPartitionIterator(
                Odps mcClient, String dbName, String tableName) {
            return mcClient.tables()
                    .get(defaultProjectName, dbName, tableName)
                    .getPartitions().iterator();
        }

        @Override
        public boolean tableExist(
                Odps mcClient, String dbName, String tableName) {
            try {
                return mcClient.tables()
                        .exists(defaultProjectName, dbName, tableName);
            } catch (OdpsException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean databaseExist(Odps mcClient, String dbName) {
            try {
                return mcClient.schemas().exists(dbName);
            } catch (OdpsException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public TableIdentifier getTableIdentifier(
                String dbName, String tableName) {
            return TableIdentifier.of(
                    defaultProjectName, dbName, tableName);
        }

        @Override
        public Table getOdpsTable(
                Odps mcClient, String dbName, String tableName) {
            return mcClient.tables()
                    .get(defaultProjectName, dbName, tableName);
        }

        @Override
        public Tables.TableCreator createTableCreator(
                Odps mcClient, String dbName,
                String tableName, TableSchema schema) {
            return mcClient.tables()
                    .newTableCreator(
                            defaultProjectName, tableName, schema)
                    .withSchemaName(dbName);
        }

        @Override
        public void dropTable(Odps mcClient, String dbName,
                String tableName, boolean ifExists) throws OdpsException {
            mcClient.tables().delete(
                    defaultProjectName, dbName, tableName, ifExists);
        }

        @Override
        public void createDb(
                Odps mcClient, String dbName, boolean ifNotExists) {
            try {
                if (ifNotExists && mcClient.schemas().exists(dbName)) {
                    return;
                }
                mcClient.schemas().create(defaultProjectName, dbName);
            } catch (OdpsException e) {
                throw new DorisConnectorException(
                        "Failed to create schema '" + dbName
                                + "': " + e.getMessage(), e);
            }
        }

        @Override
        public void dropDb(
                Odps mcClient, String dbName, boolean ifExists) {
            try {
                if (ifExists && !mcClient.schemas().exists(dbName)) {
                    return;
                }
                mcClient.schemas().delete(defaultProjectName, dbName);
            } catch (OdpsException e) {
                throw new DorisConnectorException(
                        "Failed to drop schema '" + dbName
                                + "': " + e.getMessage(), e);
            }
        }
    }

    /**
     * mc.enable.namespace.schema = false.
     * Mapping: Doris(db, table) → MC(project, table).
     */
    class ProjectTableHelper implements McStructureHelper {
        private String catalogOwner;

        @Override
        public boolean tableExist(
                Odps mcClient, String dbName, String tableName) {
            try {
                return mcClient.tables().exists(dbName, tableName);
            } catch (OdpsException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public List<String> listTableNames(Odps mcClient, String dbName) {
            List<String> result = new ArrayList<>();
            mcClient.tables().iterable(dbName)
                    .forEach(e -> result.add(e.getName()));
            return result;
        }

        @Override
        public List<String> listDatabaseNames(
                Odps mcClient, String defaultProject) {
            List<String> result = new ArrayList<>();
            result.add(defaultProject);
            try {
                if (StringUtils.isNullOrEmpty(catalogOwner)) {
                    SecurityManager sm =
                            mcClient.projects().get()
                                    .getSecurityManager();
                    String whoami = sm.runQuery("whoami", false);
                    JsonObject js = JsonParser.parseString(whoami)
                            .getAsJsonObject();
                    catalogOwner =
                            js.get("DisplayName").getAsString();
                }
                Iterator<Project> iterator =
                        mcClient.projects().iterator(catalogOwner);
                while (iterator.hasNext()) {
                    Project project = iterator.next();
                    if (!project.getName().equals(defaultProject)) {
                        result.add(project.getName());
                    }
                }
            } catch (OdpsException e) {
                throw new RuntimeException(e);
            }
            return result;
        }

        @Override
        public List<Partition> getPartitions(
                Odps mcClient, String dbName, String tableName) {
            return mcClient.tables().get(dbName, tableName)
                    .getPartitions();
        }

        @Override
        public Iterator<Partition> getPartitionIterator(
                Odps mcClient, String dbName, String tableName) {
            return mcClient.tables().get(dbName, tableName)
                    .getPartitions().iterator();
        }

        @Override
        public boolean databaseExist(Odps mcClient, String dbName) {
            try {
                return mcClient.projects().exists(dbName);
            } catch (OdpsException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public TableIdentifier getTableIdentifier(
                String dbName, String tableName) {
            return TableIdentifier.of(dbName, tableName);
        }

        @Override
        public Table getOdpsTable(
                Odps mcClient, String dbName, String tableName) {
            return mcClient.tables().get(dbName, tableName);
        }

        @Override
        public Tables.TableCreator createTableCreator(
                Odps mcClient, String dbName,
                String tableName, TableSchema schema) {
            return mcClient.tables()
                    .newTableCreator(dbName, tableName, schema);
        }

        @Override
        public void dropTable(Odps mcClient, String dbName,
                String tableName, boolean ifExists) throws OdpsException {
            mcClient.tables().delete(dbName, tableName, ifExists);
        }

        @Override
        public void createDb(
                Odps mcClient, String dbName, boolean ifNotExists) {
            throw new DorisConnectorException(
                    "Create database is not supported when "
                            + "mc.enable.namespace.schema is false.");
        }

        @Override
        public void dropDb(
                Odps mcClient, String dbName, boolean ifExists) {
            throw new DorisConnectorException(
                    "Drop database is not supported when "
                            + "mc.enable.namespace.schema is false.");
        }
    }

    static McStructureHelper getHelper(
            boolean isEnableNamespaceSchema, String defaultProjectName) {
        return isEnableNamespaceSchema
                ? new ProjectSchemaTableHelper(defaultProjectName)
                : new ProjectTableHelper();
    }
}
