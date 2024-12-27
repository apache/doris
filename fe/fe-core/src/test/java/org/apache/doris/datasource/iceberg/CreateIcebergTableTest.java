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

import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DbName;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogFactory;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class CreateIcebergTableTest {

    public static String warehouse;
    public static IcebergHadoopExternalCatalog icebergCatalog;
    public static IcebergMetadataOps ops;
    public static String dbName = "testdb";
    public static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Throwable {
        Path warehousePath = Files.createTempDirectory("test_warehouse_");
        warehouse = "file://" + warehousePath.toAbsolutePath() + "/";

        HashMap<String, String> param = new HashMap<>();
        param.put("type", "iceberg");
        param.put("iceberg.catalog.type", "hadoop");
        param.put("warehouse", warehouse);

        // create catalog
        CreateCatalogStmt createCatalogStmt = new CreateCatalogStmt(true, "iceberg", "", param, "comment");
        icebergCatalog = (IcebergHadoopExternalCatalog) CatalogFactory.createFromStmt(1, createCatalogStmt);
        if (icebergCatalog.getUseMetaCache().get()) {
            icebergCatalog.makeSureInitialized();
        } else {
            icebergCatalog.setInitialized(true);
        }

        // create db
        ops = new IcebergMetadataOps(icebergCatalog, icebergCatalog.getCatalog());
        CreateDbStmt createDbStmt = new CreateDbStmt(true, new DbName("iceberg", dbName), null);
        ops.createDb(createDbStmt);
        if (icebergCatalog.getUseMetaCache().get()) {
            icebergCatalog.makeSureInitialized();
        } else {
            icebergCatalog.setInitialized(true);
        }
        IcebergExternalDatabase db = new IcebergExternalDatabase(icebergCatalog, 1L, dbName, dbName);
        icebergCatalog.addDatabaseForTest(db);

        // context
        connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
    }

    @Test
    public void testSimpleTable() throws UserException {
        TableIdentifier tb = TableIdentifier.of(dbName, getTableName());
        String sql = "create table " + tb + " (id int) engine = iceberg";
        createTable(sql);
        Table table = ops.getCatalog().loadTable(tb);
        Schema schema = table.schema();
        Assert.assertEquals(1, schema.columns().size());
        Assert.assertEquals(PartitionSpec.unpartitioned(), table.spec());
    }

    @Test
    public void testProperties() throws UserException {
        TableIdentifier tb = TableIdentifier.of(dbName, getTableName());
        String sql = "create table " + tb + " (id int) engine = iceberg properties(\"a\"=\"b\")";
        createTable(sql);
        Table table = ops.getCatalog().loadTable(tb);
        Schema schema = table.schema();
        Assert.assertEquals(1, schema.columns().size());
        Assert.assertEquals(PartitionSpec.unpartitioned(), table.spec());
        Assert.assertEquals("b", table.properties().get("a"));
    }

    @Test
    public void testType() throws UserException {
        TableIdentifier tb = TableIdentifier.of(dbName, getTableName());
        String sql = "create table " + tb + " ("
                + "c0 int, "
                + "c1 bigint, "
                + "c2 float, "
                + "c3 double, "
                + "c4 string, "
                + "c5 date, "
                + "c6 decimal(20, 10), "
                + "c7 datetime"
                + ") engine = iceberg "
                + "properties(\"a\"=\"b\")";
        createTable(sql);
        Table table = ops.getCatalog().loadTable(tb);
        Schema schema = table.schema();
        List<Types.NestedField> columns = schema.columns();
        Assert.assertEquals(8, columns.size());
        Assert.assertEquals(Type.TypeID.INTEGER, columns.get(0).type().typeId());
        Assert.assertEquals(Type.TypeID.LONG, columns.get(1).type().typeId());
        Assert.assertEquals(Type.TypeID.FLOAT, columns.get(2).type().typeId());
        Assert.assertEquals(Type.TypeID.DOUBLE, columns.get(3).type().typeId());
        Assert.assertEquals(Type.TypeID.STRING, columns.get(4).type().typeId());
        Assert.assertEquals(Type.TypeID.DATE, columns.get(5).type().typeId());
        Assert.assertEquals(Type.TypeID.DECIMAL, columns.get(6).type().typeId());
        Assert.assertEquals(Type.TypeID.TIMESTAMP, columns.get(7).type().typeId());
    }

    @Test
    public void testPartition() throws UserException {
        TableIdentifier tb = TableIdentifier.of(dbName, getTableName());
        String sql = "create table " + tb + " ("
                + "id int, "
                + "ts1 datetime, "
                + "ts2 datetime, "
                + "ts3 datetime, "
                + "ts4 datetime, "
                + "dt1 date, "
                + "dt2 date, "
                + "dt3 date, "
                + "s string"
                + ") engine = iceberg "
                + "partition by ("
                + "id, "
                + "bucket(2, id), "
                + "year(ts1), "
                + "year(dt1), "
                + "month(ts2), "
                + "month(dt2), "
                + "day(ts3), "
                + "day(dt3), "
                + "hour(ts4), "
                + "truncate(10, s)) ()"
                + "properties(\"a\"=\"b\")";
        createTable(sql);
        Table table = ops.getCatalog().loadTable(tb);
        Schema schema = table.schema();
        Assert.assertEquals(9, schema.columns().size());
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("id")
                .bucket("id", 2)
                .year("ts1")
                .year("dt1")
                .month("ts2")
                .month("dt2")
                .day("ts3")
                .day("dt3")
                .hour("ts4")
                .truncate("s", 10)
                .build();
        Assert.assertEquals(spec, table.spec());
        Assert.assertEquals("b", table.properties().get("a"));
    }

    public void createTable(String sql) throws UserException {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof CreateTableCommand);
        CreateTableInfo createTableInfo = ((CreateTableCommand) plan).getCreateTableInfo();
        createTableInfo.setIsExternal(true);
        CreateTableStmt createTableStmt = createTableInfo.translateToLegacyStmt();
        ops.createTable(createTableStmt);
    }

    public String getTableName() {
        String s = "test_tb_" + UUID.randomUUID();
        return s.replaceAll("-", "");
    }

    @Test
    public void testDropDB() {
        String dbName = "db_to_delete";
        CreateDbStmt createDBStmt = new CreateDbStmt(false, new DbName("iceberg", dbName), new HashMap<>());
        DropDbStmt dropDbStmt = new DropDbStmt(false, new DbName("iceberg", dbName), false);
        DropDbStmt dropDbStmt2 = new DropDbStmt(false, new DbName("iceberg", "not_exists"), false);
        try {
            // create db success
            ops.createDb(createDBStmt);
            // drop db success
            ops.dropDb(dropDbStmt);
        } catch (Throwable t) {
            Assert.fail();
        }

        try {
            ops.dropDb(dropDbStmt2);
            Assert.fail();
        } catch (Throwable t) {
            Assert.assertTrue(t instanceof DdlException);
            Assert.assertTrue(t.getMessage().contains("database doesn't exist"));
        }
    }
}
