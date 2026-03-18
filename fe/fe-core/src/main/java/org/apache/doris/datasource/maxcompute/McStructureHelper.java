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

package org.apache.doris.datasource.maxcompute;


import org.apache.doris.common.DdlException;

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
 * Due to the introduction of the `mc.enable.namespace.schema` property, most interfaces using the
 * ODPS client have changed, and the mapping structure between Doris and MaxCompute has also changed.
 * Different property values correspond to different implementation class.
 * It's important to note that when external functions are called through the interface, the structure
 * mapped by Doris (database/table) is used, and the MaxCompute concept does not need to be considered.
 */
public interface McStructureHelper {
    List<String> listTableNames(Odps mcClient, String dbName);

    List<String> listDatabaseNames(Odps mcClient, String defaultProject);

    boolean tableExist(Odps mcClient, String dbName, String tableName) throws RuntimeException;

    boolean databaseExist(Odps mcClient, String dbName);

    TableIdentifier getTableIdentifier(String dbName, String tableName);

    List<Partition> getPartitions(Odps mcClient, String dbName, String tableName);

    Iterator<Partition> getPartitionIterator(Odps mcClient, String dbName, String tableName);

    Table getOdpsTable(Odps mcClient, String dbName, String tableName);

    Tables.TableCreator createTableCreator(Odps mcClient, String dbName, String tableName, TableSchema schema);

    void dropTable(Odps mcClient, String dbName, String tableName, boolean ifExists) throws OdpsException;

    void createDb(Odps mcClient, String dbName, boolean ifNotExists) throws DdlException;

    void dropDb(Odps mcClient, String dbName, boolean ifExists) throws DdlException;

    /**
     * `mc.enable.namespace.schema` = true.
     * mapping structure between Doris and MaxCompute:
     * Doris     : catalog, dbName, tableName
     * MaxCompute: project, schema, table
    */
    class ProjectSchemaTableHelper implements McStructureHelper {
        private String defaultProjectName = null;

        public ProjectSchemaTableHelper(String defaultProjectName) {
            this.defaultProjectName = defaultProjectName;
        }

        @Override
        public List<String> listTableNames(Odps mcClient, String dbName) {
            List<String> result = new ArrayList<>();
            mcClient.tables().iterable(defaultProjectName, dbName, null, false)
                    .forEach(e -> result.add(e.getName()));
            return result;
        }

        @Override
        public List<String> listDatabaseNames(Odps mcClient, String defaultProject) {
            List<String> result = new ArrayList<>();
            Iterator<Schema> iterator = mcClient.schemas().iterator(defaultProjectName);
            while (iterator.hasNext()) {
                Schema schema = iterator.next();
                result.add(schema.getName());
            }
            return result;
        }

        @Override
        public List<Partition> getPartitions(Odps mcClient, String dbName, String tableName) {
            return mcClient.tables().get(defaultProjectName, dbName, tableName).getPartitions();
        }

        @Override
        public Iterator<Partition> getPartitionIterator(Odps mcClient, String dbName, String tableName) {
            return mcClient.tables().get(defaultProjectName, dbName, tableName).getPartitions().iterator();
        }

        @Override
        public boolean tableExist(Odps mcClient, String dbName, String tableName) throws RuntimeException {
            try {
                return mcClient.tables().exists(defaultProjectName, dbName, tableName);
            } catch (OdpsException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean databaseExist(Odps mcClient, String dbName) throws RuntimeException {
            try {
                return mcClient.schemas().exists(dbName);
            } catch (OdpsException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public TableIdentifier getTableIdentifier(String dbName, String tableName) {
            return TableIdentifier.of(defaultProjectName, dbName, tableName);
        }

        @Override
        public Table getOdpsTable(Odps mcClient, String dbName, String tableName) {
            return mcClient.tables().get(defaultProjectName, dbName, tableName);
        }

        @Override
        public Tables.TableCreator createTableCreator(Odps mcClient, String dbName, String tableName,
                TableSchema schema) {
            // dbName is the schema name, defaultProjectName is the project
            return mcClient.tables().newTableCreator(defaultProjectName, tableName, schema)
                    .withSchemaName(dbName);
        }

        @Override
        public void dropTable(Odps mcClient, String dbName, String tableName, boolean ifExists)
                throws OdpsException {
            // dbName is the schema name, defaultProjectName is the project
            mcClient.tables().delete(defaultProjectName, dbName, tableName, ifExists);
        }

        @Override
        public void createDb(Odps mcClient, String dbName, boolean ifNotExists) throws DdlException {
            try {
                if (ifNotExists && mcClient.schemas().exists(dbName)) {
                    return;
                }
                mcClient.schemas().create(defaultProjectName, dbName);
            } catch (OdpsException e) {
                throw new DdlException("Failed to create schema '" + dbName + "': " + e.getMessage(), e);
            }
        }

        @Override
        public void dropDb(Odps mcClient, String dbName, boolean ifExists) throws DdlException {
            try {
                if (ifExists && !mcClient.schemas().exists(dbName)) {
                    return;
                }
                mcClient.schemas().delete(defaultProjectName, dbName);
            } catch (OdpsException e) {
                throw new DdlException("Failed to drop schema '" + dbName + "': " + e.getMessage(), e);
            }
        }
    }

    /**
     * `mc.enable.namespace.schema` = false.
     * mapping structure between Doris and MaxCompute:
     * Doris     : dbName, tableName
     * MaxCompute: project, table
     */
    class ProjectTableHelper implements McStructureHelper {
        private String catalogOwner = null;

        @Override
        public boolean tableExist(Odps mcClient, String dbName, String tableName) throws RuntimeException {
            try {
                return mcClient.tables().exists(dbName, tableName);
            } catch (OdpsException e) {
                throw new RuntimeException(e);
            }
        }


        @Override
        public List<String> listTableNames(Odps mcClient, String dbName) {
            List<String> result = new ArrayList<>();
            mcClient.tables().iterable(dbName).forEach(e -> result.add(e.getName()));
            return result;
        }

        @Override
        public List<String> listDatabaseNames(Odps mcClient, String defaultProject) {
            List<String> result = new ArrayList<>();
            result.add(defaultProject);
            try {
                result.add(defaultProject);
                if (StringUtils.isNullOrEmpty(catalogOwner)) {
                    SecurityManager sm = mcClient.projects().get().getSecurityManager();
                    String whoami = sm.runQuery("whoami", false);

                    JsonObject js = JsonParser.parseString(whoami).getAsJsonObject();
                    catalogOwner = js.get("DisplayName").getAsString();
                }
                Iterator<Project> iterator = mcClient.projects().iterator(catalogOwner);
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
        public List<Partition> getPartitions(Odps mcClient, String dbName, String tableName) {
            return mcClient.tables().get(dbName, tableName).getPartitions();
        }

        @Override
        public Iterator<Partition> getPartitionIterator(Odps mcClient, String dbName, String tableName) {
            return mcClient.tables().get(dbName, tableName).getPartitions().iterator();
        }

        @Override
        public boolean databaseExist(Odps mcClient, String dbName) throws RuntimeException {
            try {
                return mcClient.projects().exists(dbName);
            } catch (OdpsException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public TableIdentifier getTableIdentifier(String dbName, String tableName) {
            return TableIdentifier.of(dbName, tableName);
        }


        @Override
        public Table getOdpsTable(Odps mcClient, String dbName, String tableName) {
            return mcClient.tables().get(dbName, tableName);
        }

        @Override
        public Tables.TableCreator createTableCreator(Odps mcClient, String dbName, String tableName,
                TableSchema schema) {
            // dbName is the project name
            return mcClient.tables().newTableCreator(dbName, tableName, schema);
        }

        @Override
        public void dropTable(Odps mcClient, String dbName, String tableName, boolean ifExists)
                throws OdpsException {
            // dbName is the project name
            mcClient.tables().delete(dbName, tableName, ifExists);
        }

        @Override
        public void createDb(Odps mcClient, String dbName, boolean ifNotExists) throws DdlException {
            throw new DdlException(
                    "Create database is not supported when mc.enable.namespace.schema is false.");
        }

        @Override
        public void dropDb(Odps mcClient, String dbName, boolean ifExists) throws DdlException {
            throw new DdlException(
                    "Drop database is not supported when mc.enable.namespace.schema is false.");
        }
    }

    static McStructureHelper getHelper(boolean isEnableNamespaceSchema, String defaultProjectName) {
        return isEnableNamespaceSchema
                ? new ProjectSchemaTableHelper(defaultProjectName)
                : new ProjectTableHelper();
    }
}
