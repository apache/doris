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

package org.apache.doris.datasource.paimon;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.CatalogFactory;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateCatalogCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class PaimonMetadataOpsTest {
    public static String warehouse;
    public static PaimonExternalCatalog paimonCatalog;
    public static PaimonMetadataOps ops;
    public static String dbName = "test_db";
    public static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Throwable {
        Path warehousePath = Files.createTempDirectory("test_warehouse_");
        warehouse = "file://" + warehousePath.toAbsolutePath() + "/";
        HashMap<String, String> param = new HashMap<>();
        param.put("type", "paimon");
        param.put("paimon.catalog.type", "filesystem");
        param.put("warehouse", warehouse);
        // create catalog
        CreateCatalogCommand createCatalogCommand = new CreateCatalogCommand("paimon", true, "", "comment", param);
        paimonCatalog = (PaimonExternalCatalog) CatalogFactory.createFromCommand(1, createCatalogCommand);
        paimonCatalog.makeSureInitialized();
        // create db
        ops = new PaimonMetadataOps(paimonCatalog, paimonCatalog.catalog);
        ops.createDb(dbName, true, Maps.newHashMap());
        paimonCatalog.makeSureInitialized();

        // context
        connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
    }

    @Test
    public void testSimpleTable() throws Exception {
        String tableName = getTableName();
        Identifier identifier = new Identifier(dbName, tableName);
        String sql = "create table " + dbName + "." + tableName + " (id int) engine = paimon";
        createTable(sql);
        Catalog catalog = ops.getCatalog();
        Table table = catalog.getTable(identifier);
        List<String> columnNames = new ArrayList<>();
        if (catalog instanceof HiveCatalog) {
            columnNames.addAll(((HiveCatalog) catalog).loadTableSchema(identifier).fieldNames());
        } else if (catalog instanceof FileSystemCatalog) {
            columnNames.addAll(((FileSystemCatalog) catalog).loadTableSchema(identifier).fieldNames());
        }

        if (!columnNames.isEmpty()) {
            Assert.assertEquals(1, columnNames.size());
        }
        Assert.assertEquals(0, table.partitionKeys().size());
    }

    @Test
    public void testProperties() throws Exception {
        String tableName = getTableName();
        Identifier identifier = new Identifier(dbName, tableName);
        String sql = "create table " + dbName + "." + tableName + " (id int) engine = paimon properties(\"primary-key\"=id)";
        createTable(sql);
        Catalog catalog = ops.getCatalog();
        Table table = catalog.getTable(identifier);

        List<String> columnNames = new ArrayList<>();
        if (catalog instanceof HiveCatalog) {
            columnNames.addAll(((HiveCatalog) catalog).loadTableSchema(identifier).fieldNames());
        } else if (catalog instanceof FileSystemCatalog) {
            columnNames.addAll(((FileSystemCatalog) catalog).loadTableSchema(identifier).fieldNames());
        }

        if (!columnNames.isEmpty()) {
            Assert.assertEquals(1, columnNames.size());
        }
        Assert.assertEquals(0, table.partitionKeys().size());
        Assert.assertTrue(table.primaryKeys().contains("id"));
        Assert.assertEquals(1, table.primaryKeys().size());
    }

    @Test
    public void testUpdateTablePropertiesPersistsAllOptions() throws Exception {
        String tableName = getTableName();
        Identifier identifier = new Identifier(dbName, tableName);
        createTable("create table " + dbName + "." + tableName + " (id int) engine = paimon");

        ExternalCatalog dorisCatalog = Mockito.mock(ExternalCatalog.class);
        PaimonMetadataOps propertyOps = newMetadataOps(dorisCatalog, ops.getCatalog());
        ExternalTable dorisTable = mockExternalTable(tableName);
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("snapshot.num-retained.min", "2");
        properties.put("snapshot.num-retained.max", "5");

        propertyOps.updateTableProperties(dorisTable, properties, 123L);

        Map<String, String> actualOptions = ops.getCatalog().getTable(identifier).options();
        Assert.assertEquals("2", actualOptions.get("snapshot.num-retained.min"));
        Assert.assertEquals("5", actualOptions.get("snapshot.num-retained.max"));
        Mockito.verify(dorisCatalog).getDbForReplay(dbName);
    }

    @Test
    public void testUpdateTablePropertiesUsesOneAtomicAlter() throws Exception {
        String tableName = getTableName();
        Identifier identifier = new Identifier(dbName, tableName);
        Catalog remoteCatalog = Mockito.mock(Catalog.class);
        ExternalCatalog dorisCatalog = Mockito.mock(ExternalCatalog.class);
        PaimonMetadataOps propertyOps = newMetadataOps(dorisCatalog, remoteCatalog);
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("snapshot.num-retained.min", "2");
        properties.put("snapshot.num-retained.max", "5");

        propertyOps.updateTableProperties(mockExternalTable(tableName), properties, 123L);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<SchemaChange>> changesCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(remoteCatalog).alterTable(Mockito.eq(identifier), changesCaptor.capture(), Mockito.eq(false));
        Assert.assertEquals(
                java.util.Arrays.asList(
                        SchemaChange.setOption("snapshot.num-retained.min", "2"),
                        SchemaChange.setOption("snapshot.num-retained.max", "5")),
                changesCaptor.getValue());
    }

    @Test
    public void testUpdateTablePropertiesRejectsPathBeforeRemoteAlter() throws Exception {
        String tableName = getTableName();
        Catalog remoteCatalog = Mockito.mock(Catalog.class);
        ExternalCatalog dorisCatalog = Mockito.mock(ExternalCatalog.class);
        PaimonMetadataOps propertyOps = newMetadataOps(dorisCatalog, remoteCatalog);
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("snapshot.num-retained.max", "10");
        properties.put("PATH", "s3://warehouse/relocated_table");

        UserException exception = Assert.assertThrows(UserException.class,
                () -> propertyOps.updateTableProperties(mockExternalTable(tableName), properties, 123L));

        Assert.assertTrue(exception.getMessage().contains("Change path is not supported yet"));
        Mockito.verify(remoteCatalog, Mockito.never())
                .alterTable(Mockito.any(Identifier.class), Mockito.anyList(), Mockito.anyBoolean());
        Mockito.verify(dorisCatalog, Mockito.never()).getDbForReplay(Mockito.anyString());
    }

    @Test
    public void testUpdateTablePropertiesRejectsInvalidBatchAtomically() throws Exception {
        String tableName = getTableName();
        Identifier identifier = new Identifier(dbName, tableName);
        createTable("create table " + dbName + "." + tableName
                + " (id int not null, seq bigint) engine = paimon "
                + "properties ('primary-key' = 'id', 'snapshot.num-retained.min' = '2', "
                + "'snapshot.num-retained.max' = '5')");

        ExternalCatalog dorisCatalog = Mockito.mock(ExternalCatalog.class);
        PaimonMetadataOps propertyOps = newMetadataOps(dorisCatalog, ops.getCatalog());
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("fields.missing.sequence-group", "seq");
        properties.put("snapshot.num-retained.max", "10");

        UserException exception = Assert.assertThrows(UserException.class,
                () -> propertyOps.updateTableProperties(mockExternalTable(tableName), properties, 123L));

        Assert.assertTrue(exception.getMessage().toLowerCase().contains("missing"));
        ops.getCatalog().invalidateTable(identifier);
        Map<String, String> actualOptions = ops.getCatalog().getTable(identifier).options();
        Assert.assertEquals("5", actualOptions.get("snapshot.num-retained.max"));
        Assert.assertFalse(actualOptions.containsKey("fields.missing.sequence-group"));
        Mockito.verify(dorisCatalog, Mockito.never()).getDbForReplay(Mockito.anyString());
    }

    private PaimonMetadataOps newMetadataOps(ExternalCatalog dorisCatalog, Catalog remoteCatalog) {
        Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        return new PaimonMetadataOps(dorisCatalog, remoteCatalog);
    }

    private ExternalTable mockExternalTable(String tableName) {
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Mockito.when(dorisTable.getDbName()).thenReturn(dbName);
        Mockito.when(dorisTable.getRemoteDbName()).thenReturn(dbName);
        Mockito.when(dorisTable.getName()).thenReturn(tableName);
        Mockito.when(dorisTable.getRemoteName()).thenReturn(tableName);
        return dorisTable;
    }

    @Test
    public void testType() throws Exception {
        String tableName = getTableName();
        Identifier identifier = new Identifier(dbName, tableName);
        String sql = "create table " + dbName + "." + tableName + " ("
                + "c0 int, "
                + "c1 bigint, "
                + "c2 float, "
                + "c3 double, "
                + "c4 string, "
                + "c5 date, "
                + "c6 decimal(20, 10), "
                + "c7 datetime"
                + ") engine = paimon "
                + "properties(\"primary-key\"=c0)";
        createTable(sql);
        Catalog catalog = ops.getCatalog();
        Table table = catalog.getTable(identifier);

        List<DataField> columns = new ArrayList<>();
        if (catalog instanceof HiveCatalog) {
            columns.addAll(((HiveCatalog) catalog).loadTableSchema(identifier).fields());
        } else if (catalog instanceof FileSystemCatalog) {
            columns.addAll(((FileSystemCatalog) catalog).loadTableSchema(identifier).fields());
        }

        if (!columns.isEmpty()) {
            Assert.assertEquals(8, columns.size());
            Assert.assertEquals(new IntType().asSQLString(), columns.get(0).type().toString());
            Assert.assertEquals(new BigIntType().asSQLString(), columns.get(1).type().toString());
            Assert.assertEquals(new FloatType().asSQLString(), columns.get(2).type().toString());
            Assert.assertEquals(new DoubleType().asSQLString(), columns.get(3).type().toString());
            Assert.assertEquals(new VarCharType(VarCharType.MAX_LENGTH).asSQLString(), columns.get(4).type().toString());
            Assert.assertEquals(new DateType().asSQLString(), columns.get(5).type().toString());
            Assert.assertEquals(new DecimalType(20, 10).asSQLString(), columns.get(6).type().toString());
            Assert.assertEquals(new TimestampType().asSQLString(), columns.get(7).type().toString());
        }

        Assert.assertEquals(0, table.partitionKeys().size());
        Assert.assertTrue(table.primaryKeys().contains("c0"));
        Assert.assertEquals(1, table.primaryKeys().size());
    }

    @Test
    public void testPartition() throws Exception {
        String tableName = "test04";
        Identifier identifier = new Identifier(dbName, tableName);
        String sql = "create table " + dbName + "." + tableName + " ("
                + "c0 int, "
                + "c1 bigint, "
                + "c2 float, "
                + "c3 double, "
                + "c4 string, "
                + "c5 date, "
                + "c6 decimal(20, 10), "
                + "c7 datetime"
                + ") engine = paimon "
                + "partition by ("
                + "c1 ) ()"
                + "properties(\"primary-key\"=c0)";
        createTable(sql);
        Catalog catalog = ops.getCatalog();
        Table table = catalog.getTable(identifier);
        Assert.assertEquals(1, table.partitionKeys().size());
        Assert.assertTrue(table.primaryKeys().contains("c0"));
        Assert.assertEquals(1, table.primaryKeys().size());
    }

    @Test
    public void testPartitionPreservesNonLowercaseColumnNames() throws Exception {
        String tableName = getTableName();
        Identifier identifier = new Identifier(dbName, tableName);
        String sql = "create table " + dbName + "." + tableName + " ("
                + "data int, "
                + "`PART` int, "
                + "`mIxEd_COL` int"
                + ") engine = paimon "
                + "partition by (`PART`) ()";
        createTable(sql);
        Catalog catalog = ops.getCatalog();
        Table table = catalog.getTable(identifier);

        List<String> columnNames = table.rowType().getFields().stream()
                .map(DataField::name)
                .collect(Collectors.toList());

        Assert.assertEquals("PART", columnNames.get(1));
        Assert.assertEquals("mIxEd_COL", columnNames.get(2));
        Assert.assertEquals(1, table.partitionKeys().size());
        Assert.assertEquals("PART", table.partitionKeys().get(0));
    }

    @Test
    public void testBucket() throws Exception {
        String tableName = getTableName();
        Identifier identifier = new Identifier(dbName, tableName);
        String sql = "create table " + dbName + "." + tableName + " ("
                + "c0 int, "
                + "c1 bigint, "
                + "c2 float, "
                + "c3 double, "
                + "c4 string, "
                + "c5 date, "
                + "c6 decimal(20, 10), "
                + "c7 datetime"
                + ") engine = paimon "
                + "properties(\"primary-key\"=c0,"
                + "\"bucket\" = 4,"
                + "\"bucket-key\" = c0)";
        createTable(sql);
        Catalog catalog = ops.getCatalog();
        Table table = catalog.getTable(identifier);
        Assert.assertEquals("4", table.options().get("bucket"));
        Assert.assertEquals("c0", table.options().get("bucket-key"));
    }

    @Test
    public void testModifyColumnPreservesRemoteTypeBehindLossyProjection()
            throws Exception {
        TimestampType remoteType = new TimestampType(9);
        DataField remoteField = new DataField(0, "ts", remoteType);
        Column projectedColumn = new Column(
                "ts", ScalarType.createDatetimeV2Type(6), true);

        org.apache.paimon.types.DataType requestedType =
                ops.requestedColumnType(projectedColumn, remoteField);

        Assert.assertTrue(requestedType instanceof TimestampType);
        Assert.assertEquals(9, ((TimestampType) requestedType).getPrecision());
        Assert.assertEquals(projectedColumn.isAllowNull(), requestedType.isNullable());
    }

    @Test
    public void testIfNotExistsRefreshesNamesWhenRemoteTableExists() throws Exception {
        String tableName = getTableName();
        Catalog remoteCatalog = Mockito.mock(Catalog.class);
        ExternalCatalog dorisCatalog = Mockito.mock(ExternalCatalog.class);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        Mockito.doReturn(database).when(dorisCatalog).getDbNullable(dbName);
        Mockito.doReturn(Optional.of(database)).when(dorisCatalog).getDbForReplay(dbName);
        Mockito.when(database.getRemoteName()).thenReturn(dbName);

        PaimonMetadataOps existingTableOps = new PaimonMetadataOps(dorisCatalog, remoteCatalog) {
            @Override
            public boolean tableExist(String ignoredDbName, String ignoredTableName) {
                return true;
            }
        };
        CreateTableInfo createTableInfo = parseCreateTableInfo(
                "create table if not exists " + dbName + "." + tableName + " (id int) engine = paimon");

        Assert.assertTrue(existingTableOps.performCreateTable(createTableInfo));
        Mockito.verify(database).resetMetaCacheNames();
        Mockito.verify(remoteCatalog, Mockito.never()).createTable(
                Mockito.any(Identifier.class), Mockito.any(Schema.class), Mockito.anyBoolean());
    }

    @Test
    public void testIfNotExistsReportsConcurrentWinner() throws Exception {
        String tableName = getTableName();
        Identifier identifier = new Identifier(dbName, tableName);
        Catalog remoteCatalog = Mockito.mock(Catalog.class);
        ExternalCatalog dorisCatalog = Mockito.mock(ExternalCatalog.class);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        Mockito.doReturn(database).when(dorisCatalog).getDbNullable(dbName);
        Mockito.doReturn(Optional.of(database)).when(dorisCatalog).getDbForReplay(dbName);
        Mockito.when(database.getRemoteName()).thenReturn(dbName);
        Mockito.when(database.getTableNullable(tableName)).thenReturn(null);

        PaimonMetadataOps raceOps = new PaimonMetadataOps(dorisCatalog, remoteCatalog) {
            @Override
            public boolean tableExist(String ignoredDbName, String ignoredTableName) {
                return false;
            }
        };
        CreateTableInfo createTableInfo = parseCreateTableInfo(
                "create table if not exists " + dbName + "." + tableName + " (id int) engine = paimon");
        Mockito.doThrow(new Catalog.TableAlreadyExistException(identifier))
                .when(remoteCatalog).createTable(
                        Mockito.eq(identifier), Mockito.any(Schema.class), Mockito.eq(false));

        Assert.assertTrue(raceOps.performCreateTable(createTableInfo));
        Mockito.verify(database).resetMetaCacheNames();
    }

    public void createTable(String sql) throws UserException {
        ops.createTable(parseCreateTableInfo(sql));
    }

    private CreateTableInfo parseCreateTableInfo(String sql) throws UserException {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof CreateTableCommand);
        CreateTableInfo createTableInfo = ((CreateTableCommand) plan).getCreateTableInfo();
        createTableInfo.setIsExternal(true);
        createTableInfo.analyzeEngine();
        return createTableInfo;
    }

    public String getTableName() {
        String s = "test_tb_" + UUID.randomUUID();
        return s.replaceAll("-", "");
    }

    @Test
    public void testDropDB() {
        try {
            // create db success
            ops.createDb("t_paimon", false, Maps.newHashMap());
            // drop db success
            ops.dropDb("t_paimon", false, false);
        } catch (Throwable t) {
            Assert.fail();
        }

        try {
            ops.dropDb("t_paimon", false, false);
            Assert.fail();
        } catch (Throwable t) {
            Assert.assertTrue(t instanceof DdlException);
            Assert.assertTrue(t.getMessage().contains("database doesn't exist"));
        }
    }

    @Test
    public void testCreateDatabaseWithPropertiesForSupportedCatalogs() throws Exception {
        List<String> supportedCatalogTypes = Arrays.asList(
                PaimonExternalCatalog.PAIMON_HMS,
                PaimonExternalCatalog.PAIMON_JDBC,
                PaimonExternalCatalog.PAIMON_REST,
                PaimonExternalCatalog.PAIMON_DLF);
        for (String catalogType : supportedCatalogTypes) {
            String remoteDbName = catalogType + "_db";
            Catalog remoteCatalog = Mockito.mock(Catalog.class);
            PaimonExternalCatalog dorisCatalog = Mockito.mock(PaimonExternalCatalog.class);
            Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {});
            Mockito.when(dorisCatalog.getCatalogType()).thenReturn(catalogType);
            Mockito.doThrow(new Catalog.DatabaseNotExistException(remoteDbName))
                    .when(remoteCatalog).getDatabase(remoteDbName);
            PaimonMetadataOps catalogOps = new PaimonMetadataOps(dorisCatalog, remoteCatalog);
            HashMap<String, String> properties = Maps.newHashMap();
            properties.put("owner", "doris");

            Assert.assertFalse(catalogOps.createDbImpl(remoteDbName, false, properties));

            Mockito.verify(remoteCatalog).createDatabase(remoteDbName, false, properties);
        }
    }

    @Test
    public void testCreateDatabaseWithLocationForSupportedCatalogs() throws Exception {
        List<String> supportedCatalogTypes = Arrays.asList(
                PaimonExternalCatalog.PAIMON_HMS,
                PaimonExternalCatalog.PAIMON_DLF);
        for (String catalogType : supportedCatalogTypes) {
            String remoteDbName = catalogType + "_location_db";
            Catalog remoteCatalog = Mockito.mock(Catalog.class);
            PaimonExternalCatalog dorisCatalog = Mockito.mock(PaimonExternalCatalog.class);
            Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {});
            Mockito.when(dorisCatalog.getCatalogType()).thenReturn(catalogType);
            Mockito.doThrow(new Catalog.DatabaseNotExistException(remoteDbName))
                    .when(remoteCatalog).getDatabase(remoteDbName);
            PaimonMetadataOps catalogOps = new PaimonMetadataOps(dorisCatalog, remoteCatalog);
            HashMap<String, String> properties = Maps.newHashMap();
            properties.put("location", "s3://warehouse/" + remoteDbName);

            Assert.assertFalse(catalogOps.createDbImpl(remoteDbName, false, properties));

            Mockito.verify(remoteCatalog).createDatabase(remoteDbName, false, properties);
        }
    }

    @Test
    public void testCreateDatabaseWithLocationForCatalogsThatIgnoreItIsRejected() throws Exception {
        List<String> unsupportedCatalogTypes = Arrays.asList(
                PaimonExternalCatalog.PAIMON_JDBC,
                PaimonExternalCatalog.PAIMON_REST);
        for (String catalogType : unsupportedCatalogTypes) {
            String remoteDbName = catalogType + "_location_db";
            Catalog remoteCatalog = Mockito.mock(Catalog.class);
            PaimonExternalCatalog dorisCatalog = Mockito.mock(PaimonExternalCatalog.class);
            Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {});
            Mockito.when(dorisCatalog.getCatalogType()).thenReturn(catalogType);
            Mockito.doThrow(new Catalog.DatabaseNotExistException(remoteDbName))
                    .when(remoteCatalog).getDatabase(remoteDbName);
            PaimonMetadataOps catalogOps = new PaimonMetadataOps(dorisCatalog, remoteCatalog);
            HashMap<String, String> properties = Maps.newHashMap();
            properties.put("location", "s3://warehouse/" + remoteDbName);

            DdlException exception = Assert.assertThrows(
                    DdlException.class,
                    () -> catalogOps.createDbImpl(remoteDbName, false, properties));

            Assert.assertTrue(exception.getMessage().contains(
                    "database property 'location' for paimon catalog type: " + catalogType));
            Mockito.verify(remoteCatalog, Mockito.never())
                    .createDatabase(Mockito.anyString(), Mockito.anyBoolean(), Mockito.anyMap());
        }
    }

    @Test
    public void testCreateDatabaseWithPropertiesForFilesystemCatalogIsRejected() throws Exception {
        String filesystemDbName = "filesystem_db";
        Catalog remoteCatalog = Mockito.mock(Catalog.class);
        PaimonExternalCatalog dorisCatalog = Mockito.mock(PaimonExternalCatalog.class);
        Mockito.when(dorisCatalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {});
        Mockito.when(dorisCatalog.getCatalogType()).thenReturn(PaimonExternalCatalog.PAIMON_FILESYSTEM);
        Mockito.doThrow(new Catalog.DatabaseNotExistException(filesystemDbName))
                .when(remoteCatalog).getDatabase(filesystemDbName);
        PaimonMetadataOps filesystemOps = new PaimonMetadataOps(dorisCatalog, remoteCatalog);
        HashMap<String, String> properties = Maps.newHashMap();
        properties.put("owner", "doris");

        DdlException exception = Assert.assertThrows(
                DdlException.class,
                () -> filesystemOps.createDbImpl(filesystemDbName, false, properties));

        Assert.assertTrue(exception.getMessage().contains("paimon catalog type: filesystem"));
        Mockito.verify(remoteCatalog, Mockito.never())
                .createDatabase(Mockito.anyString(), Mockito.anyBoolean(), Mockito.anyMap());
    }
}
