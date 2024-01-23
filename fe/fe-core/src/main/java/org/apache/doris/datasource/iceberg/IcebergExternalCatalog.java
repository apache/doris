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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.external.iceberg.util.DorisTypeToType;
import org.apache.doris.external.iceberg.util.DorisTypeVisitor;
import org.apache.doris.external.iceberg.util.IcebergUtils;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class IcebergExternalCatalog extends ExternalCatalog {

    private static final Logger LOG = LogManager.getLogger(IcebergExternalCatalog.class);
    public static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";
    public static final String ICEBERG_REST = "rest";
    public static final String ICEBERG_HMS = "hms";
    public static final String ICEBERG_HADOOP = "hadoop";
    public static final String ICEBERG_GLUE = "glue";
    public static final String ICEBERG_DLF = "dlf";
    protected String icebergCatalogType;
    protected Catalog catalog;
    protected SupportsNamespaces nsCatalog;

    public IcebergExternalCatalog(long catalogId, String name, String comment) {
        super(catalogId, name, InitCatalogLog.Type.ICEBERG, comment);
    }

    @Override
    protected void init() {
        nsCatalog = (SupportsNamespaces) catalog;
        super.init();
    }

    public Catalog getCatalog() {
        makeSureInitialized();
        return catalog;
    }

    public SupportsNamespaces getNsCatalog() {
        makeSureInitialized();
        return nsCatalog;
    }

    public String getIcebergCatalogType() {
        makeSureInitialized();
        return icebergCatalogType;
    }

    protected List<String> listDatabaseNames() {
        return nsCatalog.listNamespaces().stream()
            .map(e -> {
                String dbName = e.toString();
                try {
                    FeNameFormat.checkDbName(dbName);
                } catch (AnalysisException ex) {
                    Util.logAndThrowRuntimeException(LOG,
                            String.format("Not a supported namespace name format: %s", dbName), ex);
                }
                return dbName;
            })
            .collect(Collectors.toList());
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        return catalog.tableExists(TableIdentifier.of(dbName, tblName));
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        List<TableIdentifier> tableIdentifiers = catalog.listTables(Namespace.of(dbName));
        return tableIdentifiers.stream().map(TableIdentifier::name).collect(Collectors.toList());
    }

    public org.apache.iceberg.Table getIcebergTable(String dbName, String tblName) {
        makeSureInitialized();
        return Env.getCurrentEnv()
                .getExtMetaCacheMgr()
                .getIcebergMetadataCache()
                .getIcebergTable(catalog, id, dbName, tblName, getProperties());
    }

    @Override
    public void createDb(CreateDbStmt stmt) throws DdlException {
        makeSureInitialized();
        SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
        String dbName = stmt.getFullDbName();
        Map<String, String> properties = stmt.getProperties();
        nsCatalog.createNamespace(Namespace.of(dbName), properties);
        // TODO 增加刷新流程,否则create之后，show不出来，只能refresh之后才能show出来
    }

    @Override
    public void dropDb(DropDbStmt stmt) throws DdlException {
        makeSureInitialized();
        SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
        String dbName = stmt.getDbName();
        if (dbNameToId.containsKey(dbName)) {
            Long aLong = dbNameToId.get(dbName);
            idToDb.remove(aLong);
            dbNameToId.remove(dbName);
        }
        nsCatalog.dropNamespace(Namespace.of(dbName));
    }

    @Override
    public void createTable(CreateTableStmt stmt) throws UserException {
        makeSureInitialized();
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();
        List<StructField> collect = columns.stream()
                .map(col -> new StructField(col.getName(), col.getType(), col.getComment(), col.isAllowNull()))
                .collect(Collectors.toList());
        StructType structType = new StructType(new ArrayList<>(collect));
        org.apache.iceberg.types.Type visit = DorisTypeVisitor.visit(structType, new DorisTypeToType(structType));
        Schema schema = new Schema(visit.asNestedType().asStructType().fields());
        Map<String, String> properties = stmt.getProperties();
        PartitionSpec partitionSpec = IcebergUtils.solveIcebergPartitionSpec(properties, schema);
        catalog.createTable(TableIdentifier.of(dbName, tableName), schema, partitionSpec, properties);
    }

    @Override
    public void dropTable(DropTableStmt stmt) throws DdlException {
        makeSureInitialized();
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();
        catalog.dropTable(TableIdentifier.of(dbName, tableName));
    }
}
