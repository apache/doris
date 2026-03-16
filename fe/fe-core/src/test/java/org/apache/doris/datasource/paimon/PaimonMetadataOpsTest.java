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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogFactory;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

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

    public void createTable(String sql) throws UserException {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof CreateTableCommand);
        CreateTableInfo createTableInfo = ((CreateTableCommand) plan).getCreateTableInfo();
        createTableInfo.setIsExternal(true);
        createTableInfo.analyzeEngine();
        ops.createTable(createTableInfo);
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
}
