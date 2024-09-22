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

package org.apache.doris.datasource.hive;

import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DbName;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.analysis.SwitchStmt;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.Config;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.DatabaseMetadata;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.TableMetadata;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.DistributionSpecTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.DistributionSpecTableSinkRandomPartitioned;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.UnboundLogicalSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHiveTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.utframe.TestWithFeService;

import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class HiveDDLAndDMLPlanTest extends TestWithFeService {
    private static final String mockedCtlName = "hive";
    private static final String mockedDbName = "mockedDb";
    private final NereidsParser nereidsParser = new NereidsParser();

    @Mocked
    private ThriftHMSCachedClient mockedHiveClient;

    private List<FieldSchema> checkedHiveCols;

    private final Set<String> createdDbs = new HashSet<>();
    private final Set<Table> createdTables = new HashSet<>();

    @Override
    protected void runBeforeAll() throws Exception {
        Config.enable_query_hive_views = false;
        // create test internal table
        createDatabase(mockedDbName);
        useDatabase(mockedDbName);
        String createSourceInterTable = "CREATE TABLE `unpart_ctas_olap`(\n"
                + "  `col1` INT COMMENT 'col1',\n"
                + "  `col2` STRING COMMENT 'col2'\n"
                + ")  ENGINE=olap\n"
                + "DISTRIBUTED BY HASH (col1) BUCKETS 16\n"
                + "PROPERTIES (\n"
                + "  'replication_num' = '1')";
        createTable(createSourceInterTable, true);

        // partitioned table
        String createSourceInterPTable = "CREATE TABLE `part_ctas_olap`(\n"
                + "  `col1` INT COMMENT 'col1',\n"
                + "  `pt1` VARCHAR(16) COMMENT 'pt1',\n"
                + "  `pt2` VARCHAR(16) COMMENT 'pt2'\n"
                + ")  ENGINE=olap\n"
                + "PARTITION BY LIST (pt1, pt2) ()\n"
                + "DISTRIBUTED BY HASH (col1) BUCKETS 16\n"
                + "PROPERTIES (\n"
                + "  'replication_num' = '1')";
        createTable(createSourceInterPTable, true);

        // create external catalog and switch it
        CreateCatalogStmt hiveCatalog = createStmt("create catalog " + mockedCtlName
                + " properties('type' = 'hms',"
                + " 'hive.metastore.uris' = 'thrift://192.168.0.1:9083');");
        Env.getCurrentEnv().getCatalogMgr().createCatalog(hiveCatalog);
        switchHive();

        // create db and use it
        Map<String, String> dbProps = new HashMap<>();
        dbProps.put(HiveMetadataOps.LOCATION_URI_KEY, "file://loc/db");
        new MockUp<ThriftHMSCachedClient>(ThriftHMSCachedClient.class) {
            @Mock
            public void createDatabase(DatabaseMetadata db) {
                if (db instanceof HiveDatabaseMetadata) {
                    Database hiveDb = HiveUtil.toHiveDatabase((HiveDatabaseMetadata) db);
                    createdDbs.add(hiveDb.getName());
                }
            }

            @Mock
            public Database getDatabase(String dbName) {
                if (createdDbs.contains(dbName)) {
                    return new Database(dbName, "", "", null);
                }
                return null;
            }

            @Mock
            public boolean tableExists(String dbName, String tblName) {
                for (Table table : createdTables) {
                    if (table.getDbName().equals(dbName) && table.getTableName().equals(tblName)) {
                        return true;
                    }
                }
                return false;
            }

            @Mock
            public List<String> getAllDatabases() {
                return new ArrayList<>(createdDbs);
            }

            @Mock
            public void createTable(TableMetadata tbl, boolean ignoreIfExists) {
                if (tbl instanceof HiveTableMetadata) {
                    Table table = HiveUtil.toHiveTable((HiveTableMetadata) tbl);
                    createdTables.add(table);
                    if (checkedHiveCols == null) {
                        // if checkedHiveCols is null, skip column check
                        return;
                    }
                    List<FieldSchema> fieldSchemas = table.getSd().getCols();
                    Assertions.assertEquals(checkedHiveCols.size(), fieldSchemas.size());
                    for (int i = 0; i < checkedHiveCols.size(); i++) {
                        FieldSchema checkedCol = checkedHiveCols.get(i);
                        FieldSchema actualCol = fieldSchemas.get(i);
                        Assertions.assertEquals(checkedCol.getName(), actualCol.getName().toLowerCase());
                        Assertions.assertEquals(checkedCol.getType(), actualCol.getType().toLowerCase());
                    }
                }
            }

            @Mock
            public Table getTable(String dbName, String tblName) {
                for (Table createdTable : createdTables) {
                    if (createdTable.getDbName().equals(dbName) && createdTable.getTableName().equals(tblName)) {
                        return createdTable;
                    }
                }
                return null;
            }
        };
        CreateDbStmt createDbStmt = new CreateDbStmt(true, new DbName("hive", mockedDbName), dbProps);
        Env.getCurrentEnv().createDb(createDbStmt);
        // checkout ifNotExists
        Env.getCurrentEnv().createDb(createDbStmt);
        useDatabase(mockedDbName);

        // un-partitioned table
        String createSourceExtUTable = "CREATE TABLE `unpart_ctas_src`(\n"
                + "  `col1` INT COMMENT 'col1',\n"
                + "  `col2` STRING COMMENT 'col2'\n"
                + ")  ENGINE=hive\n"
                + "PROPERTIES (\n"
                + "  'location'='hdfs://loc/db/tbl',\n"
                + "  'file_format'='parquet')";
        createTable(createSourceExtUTable, true);
        // partitioned table
        String createSourceExtTable = "CREATE TABLE `part_ctas_src`(\n"
                + "  `col1` INT COMMENT 'col1',\n"
                + "  `pt1` VARCHAR COMMENT 'pt1',\n"
                + "  `pt2` VARCHAR COMMENT 'pt2'\n"
                + ")  ENGINE=hive\n"
                + "PARTITION BY LIST (pt1, pt2) ()\n"
                + "PROPERTIES (\n"
                + "  'location'='hdfs://loc/db/tbl',\n"
                + "  'file_format'='orc')";
        createTable(createSourceExtTable, true);

        HMSExternalCatalog hmsExternalCatalog = (HMSExternalCatalog) Env.getCurrentEnv().getCatalogMgr()
                .getCatalog(mockedCtlName);
        new MockUp<HMSExternalCatalog>(HMSExternalCatalog.class) {
            // mock after ThriftHMSCachedClient is mocked
            @Mock
            public ExternalDatabase<? extends ExternalTable> getDbNullable(String dbName) {
                if (createdDbs.contains(dbName)) {
                    return new HMSExternalDatabase(hmsExternalCatalog, RandomUtils.nextLong(), dbName);
                }
                return null;
            }
        };
        new MockUp<HMSExternalDatabase>(HMSExternalDatabase.class) {
            // mock after ThriftHMSCachedClient is mocked
            @Mock
            HMSExternalTable getTableNullable(String tableName) {
                for (Table table : createdTables) {
                    if (table.getTableName().equals(tableName)) {
                        return new HMSExternalTable(0, tableName, mockedDbName, hmsExternalCatalog);
                    }
                }
                return null;
            }
        };
        new MockUp<HMSExternalTable>(HMSExternalTable.class) {
            // mock after ThriftHMSCachedClient is mocked
        };
    }

    private void switchHive() throws Exception {
        SwitchStmt switchHive = (SwitchStmt) parseAndAnalyzeStmt("switch hive;");
        Env.getCurrentEnv().changeCatalog(connectContext, switchHive.getCatalogName());
    }

    private void switchInternal() throws Exception {
        SwitchStmt switchInternal = (SwitchStmt) parseAndAnalyzeStmt("switch internal;");
        Env.getCurrentEnv().changeCatalog(connectContext, switchInternal.getCatalogName());
    }

    @Override
    protected void runAfterAll() throws Exception {
        switchHive();
        String createDbStmtStr = "DROP DATABASE IF EXISTS " + mockedDbName;
        DropDbStmt createDbStmt = (DropDbStmt) parseAndAnalyzeStmt(createDbStmtStr);
        Env.getCurrentEnv().dropDb(createDbStmt);
        // check IF EXISTS
        Env.getCurrentEnv().dropDb(createDbStmt);
    }

    @Test
    public void testExistsDbOrTbl() throws Exception {
        switchHive();
        String db = "exists_db";
        String createDbStmtStr = "CREATE DATABASE IF NOT EXISTS " + db;
        createDatabaseWithSql(createDbStmtStr);
        createDatabaseWithSql(createDbStmtStr);
        useDatabase(db);

        String createTableIfNotExists = "CREATE TABLE IF NOT EXISTS test_tbl(\n"
                + "  `col1` BOOLEAN COMMENT 'col1',"
                + "  `col2` INT COMMENT 'col2'"
                + ")  ENGINE=hive\n"
                + "PROPERTIES (\n"
                + "  'location'='hdfs://loc/db/tbl',\n"
                + "  'file_format'='orc')";
        createTable(createTableIfNotExists, true);
        createTable(createTableIfNotExists, true);

        dropTableWithSql("DROP TABLE IF EXISTS test_tbl");
        dropTableWithSql("DROP TABLE IF EXISTS test_tbl");

        String dropDbStmtStr = "DROP DATABASE IF EXISTS " + db;
        dropDatabaseWithSql(dropDbStmtStr);
        dropDatabaseWithSql(dropDbStmtStr);
    }

    @Test
    public void testCreateAndDropWithSql() throws Exception {
        switchHive();
        useDatabase(mockedDbName);
        Optional<?> hiveDb = Env.getCurrentEnv().getCurrentCatalog().getDb(mockedDbName);
        Assertions.assertTrue(hiveDb.isPresent());
        Assertions.assertTrue(hiveDb.get() instanceof HMSExternalDatabase);

        String createUnPartTable = "CREATE TABLE unpart_tbl(\n"
                + "  `col1` BOOLEAN COMMENT 'col1',\n"
                + "  `col2` INT COMMENT 'col2',\n"
                + "  `col3` BIGINT COMMENT 'col3',\n"
                + "  `col4` DECIMAL(5,2) COMMENT 'col4',\n"
                + "  `pt1` STRING COMMENT 'pt1',\n"
                + "  `pt2` STRING COMMENT 'pt2'\n"
                + ")  ENGINE=hive\n"
                + "PROPERTIES (\n"
                + "  'location'='hdfs://loc/db/tbl',\n"
                + "  'file_format'='orc')";
        createTable(createUnPartTable, true);
        dropTable("unpart_tbl", true);

        String createPartTable = "CREATE TABLE IF NOT EXISTS `part_tbl`(\n"
                + "  `col1` BOOLEAN COMMENT 'col1',\n"
                + "  `col2` INT COMMENT 'col2',\n"
                + "  `col3` BIGINT COMMENT 'col3',\n"
                + "  `col4` DECIMAL(5,2) COMMENT 'col4',\n"
                + "  `col5` DATE COMMENT 'col5',\n"
                + "  `col6` DATETIME COMMENT 'col6',\n"
                + "  `pt1` VARCHAR(16) COMMENT 'pt1',\n"
                + "  `pt2` STRING COMMENT 'pt2'\n"
                + ")  ENGINE=hive\n"
                + "PARTITION BY LIST (pt1, pt2) ()\n"
                + "PROPERTIES (\n"
                + "  'location'='hdfs://loc/db/tbl',\n"
                + "  'file_format'='parquet')";
        createTable(createPartTable, true);
        // check IF NOT EXISTS
        createTable(createPartTable, true);
        dropTable("part_tbl", true);

        String createBucketedTableErr = "CREATE TABLE `err_buck_tbl`(\n"
                + "  `col1` BOOLEAN COMMENT 'col1',\n"
                + "  `col2` INT COMMENT 'col2',\n"
                + "  `col3` BIGINT COMMENT 'col3',\n"
                + "  `col4` DECIMAL(5,2) COMMENT 'col4'\n"
                + ")  ENGINE=hive\n"
                + "DISTRIBUTED BY HASH (col2) BUCKETS 16\n"
                + "PROPERTIES (\n"
                + "  'location'='hdfs://loc/db/tbl',\n"
                + "  'file_format'='orc')";
        ExceptionChecker.expectThrowsWithMsg(org.apache.doris.common.UserException.class,
                "errCode = 2, detailMessage = errCode = 2,"
                        + " detailMessage = Create hive bucket table need set enable_create_hive_bucket_table to true",
                () -> createTable(createBucketedTableErr, true));

        Config.enable_create_hive_bucket_table = true;
        String createBucketedTableOk1 = "CREATE TABLE `buck_tbl`(\n"
                + "  `col1` BOOLEAN COMMENT 'col1',\n"
                + "  `col2` INT COMMENT 'col2',\n"
                + "  `col3` BIGINT COMMENT 'col3',\n"
                + "  `col4` DECIMAL(5,2) COMMENT 'col4'\n"
                + ")  ENGINE=hive\n"
                + "DISTRIBUTED BY HASH (col2) BUCKETS 16\n"
                + "PROPERTIES (\n"
                + "  'location'='hdfs://loc/db/tbl',\n"
                + "  'file_format'='orc')";
        createTable(createBucketedTableOk1, true);
        dropTable("buck_tbl", true);

        String createBucketedTableOk2 = "CREATE TABLE `part_buck_tbl`(\n"
                + "  `col1` BOOLEAN COMMENT 'col1',\n"
                + "  `col2` INT COMMENT 'col2',\n"
                + "  `col3` BIGINT COMMENT 'col3',\n"
                + "  `col4` DECIMAL(5,2) COMMENT 'col4',\n"
                + "  `pt1` VARCHAR(16) COMMENT 'pt1',\n"
                + "  `pt2` STRING COMMENT 'pt2'\n"
                + ")  ENGINE=hive\n"
                + "PARTITION BY LIST (pt2) ()\n"
                + "DISTRIBUTED BY HASH (col2) BUCKETS 16\n"
                + "PROPERTIES (\n"
                + "  'location'='hdfs://loc/db/tbl',\n"
                + "  'file_format'='orc')";
        createTable(createBucketedTableOk2, true);
        dropTable("part_buck_tbl", true);
    }

    @Test
    public void testCTASPlanSql() throws Exception {
        switchHive();
        useDatabase(mockedDbName);
        // external to external table
        String ctas1 = "CREATE TABLE hive_ctas1 AS SELECT col1 FROM unpart_ctas_src WHERE col2='a';";
        LogicalPlan st1 = nereidsParser.parseSingle(ctas1);
        Assertions.assertTrue(st1 instanceof CreateTableCommand);
        // ((CreateTableCommand) st1).run(connectContext, null);
        String its1 = "INSERT INTO hive_ctas1 SELECT col1 FROM unpart_ctas_src WHERE col2='a';";
        LogicalPlan it1 = nereidsParser.parseSingle(its1);
        Assertions.assertTrue(it1 instanceof InsertIntoTableCommand);
        // ((InsertIntoTableCommand) it1).run(connectContext, null);
        // partitioned table
        String ctasU1 = "CREATE TABLE hive_ctas2 AS SELECT col1,pt1,pt2 FROM part_ctas_src WHERE col1>0;";
        LogicalPlan stU1 = nereidsParser.parseSingle(ctasU1);
        Assertions.assertTrue(stU1 instanceof CreateTableCommand);
        // ((CreateTableCommand) stU1).run(connectContext, null);
        String itsp1 = "INSERT INTO hive_ctas2 SELECT col1,pt1,pt2 FROM part_ctas_src WHERE col1>0;";
        LogicalPlan itp1 = nereidsParser.parseSingle(itsp1);
        Assertions.assertTrue(itp1 instanceof InsertIntoTableCommand);
        // ((InsertIntoTableCommand) itp1).run(connectContext, null);

        // external to internal table
        switchInternal();
        useDatabase(mockedDbName);
        String ctas2 = "CREATE TABLE olap_ctas1 AS SELECT col1,col2 FROM hive.mockedDb.unpart_ctas_src WHERE col2='a';";
        LogicalPlan st2 = nereidsParser.parseSingle(ctas2);
        Assertions.assertTrue(st2 instanceof CreateTableCommand);
        // ((CreateTableCommand) st2).run(connectContext, null);

        // partitioned table
        String ctasU2 = "CREATE TABLE olap_ctas2 AS SELECT col1,pt1,pt2 FROM hive.mockedDb.part_ctas_src WHERE col1>0;";
        LogicalPlan stU2 = nereidsParser.parseSingle(ctasU2);
        Assertions.assertTrue(stU2 instanceof CreateTableCommand);
        // ((CreateTableCommand) stU2).run(connectContext, null);

        // internal to external table
        String ctas3 = "CREATE TABLE hive.mockedDb.ctas_o1 AS SELECT col1,col2 FROM unpart_ctas_olap WHERE col2='a';";
        LogicalPlan st3 = nereidsParser.parseSingle(ctas3);
        Assertions.assertTrue(st3 instanceof CreateTableCommand);
        // ((CreateTableCommand) st3).run(connectContext, null);

        String its2 = "INSERT INTO hive.mockedDb.ctas_o1 SELECT col1,col2 FROM unpart_ctas_olap WHERE col2='a';";
        LogicalPlan it2 = nereidsParser.parseSingle(its2);
        Assertions.assertTrue(it2 instanceof InsertIntoTableCommand);
        // ((InsertIntoTableCommand) it2).run(connectContext, null);

        String ctasP3 = "CREATE TABLE hive.mockedDb.ctas_o2 AS SELECT col1,pt1,pt2 FROM part_ctas_olap WHERE col1>0;";
        LogicalPlan stP3 = nereidsParser.parseSingle(ctasP3);
        Assertions.assertTrue(stP3 instanceof CreateTableCommand);
        // ((CreateTableCommand) stP3).run(connectContext, null);

        String itsp2 = "INSERT INTO hive.mockedDb.ctas_o2 SELECT col1,pt1,pt2 FROM part_ctas_olap WHERE col1>0;";
        LogicalPlan itp2 = nereidsParser.parseSingle(itsp2);
        Assertions.assertTrue(itp2 instanceof InsertIntoTableCommand);
        // ((InsertIntoTableCommand) itp2).run(connectContext, null);

        // test olap CTAS in hive catalog
        FeConstants.runningUnitTest = true;
        String createOlapSrc = "CREATE TABLE `olap_src`(\n"
                + "  `col1` BOOLEAN COMMENT 'col1',\n"
                + "  `col2` INT COMMENT 'col2',\n"
                + "  `col3` BIGINT COMMENT 'col3',\n"
                + "  `col4` DECIMAL(5,2) COMMENT 'col4'\n"
                + ")\n"
                + "DISTRIBUTED BY HASH (col1) BUCKETS 100\n"
                + "PROPERTIES (\n"
                + "  'replication_num' = '1')";
        createTable(createOlapSrc, true);
        switchHive();
        useDatabase(mockedDbName);
        String olapCtasErr = "CREATE TABLE no_buck_olap ENGINE=olap AS SELECT * FROM internal.mockedDb.olap_src";
        LogicalPlan olapCtasErrPlan = nereidsParser.parseSingle(olapCtasErr);
        Assertions.assertTrue(olapCtasErrPlan instanceof CreateTableCommand);
        ExceptionChecker.expectThrowsWithMsg(org.apache.doris.nereids.exceptions.AnalysisException.class,
                "Cannot create olap table out of internal catalog."
                        + " Make sure 'engine' type is specified when use the catalog: hive",
                () -> ((CreateTableCommand) olapCtasErrPlan).run(connectContext, null));

        String olapCtasOk = "CREATE TABLE internal.mockedDb.no_buck_olap ENGINE=olap"
                + " PROPERTIES('replication_num' = '1')"
                + " AS SELECT * FROM internal.mockedDb.olap_src";
        LogicalPlan olapCtasOkPlan = createTablesAndReturnPlans(true, olapCtasOk).get(0);
        CreateTableStmt stmt = ((CreateTableCommand) olapCtasOkPlan).getCreateTableInfo().translateToLegacyStmt();
        Assertions.assertTrue(stmt.getDistributionDesc() instanceof HashDistributionDesc);
        Assertions.assertEquals(10, stmt.getDistributionDesc().getBuckets());
        // ((CreateTableCommand) olapCtasOkPlan).run(connectContext, null);

        String olapCtasOk2 = "CREATE TABLE internal.mockedDb.no_buck_olap2 DISTRIBUTED BY HASH (col1) BUCKETS 16"
                + " PROPERTIES('replication_num' = '1')"
                + " AS SELECT * FROM internal.mockedDb.olap_src";
        LogicalPlan olapCtasOk2Plan = createTablesAndReturnPlans(true, olapCtasOk2).get(0);
        CreateTableStmt stmt2 = ((CreateTableCommand) olapCtasOk2Plan).getCreateTableInfo().translateToLegacyStmt();
        Assertions.assertTrue(stmt2.getDistributionDesc() instanceof HashDistributionDesc);
        Assertions.assertEquals(16, stmt2.getDistributionDesc().getBuckets());
    }

    private static void mockTargetTable(List<Column> schema, Set<String> partNames) {
        new MockUp<HMSExternalTable>(HMSExternalTable.class) {
            @Mock
            public boolean isView() {
                return false;
            }

            @Mock
            public List<Column> getFullSchema() {
                return schema;
            }

            @Mock
            public Set<String> getPartitionColumnNames() {
                return partNames;
            }
        };
    }

    @Test
    public void testInsertIntoPlanSql() throws Exception {
        switchHive();
        useDatabase(mockedDbName);
        String insertTable = "insert_table";
        createTargetTable(insertTable);

        // test un-partitioned table
        List<Column> schema = new ArrayList<Column>() {
            {
                add(new Column("col1", PrimitiveType.INT));
                add(new Column("col2", PrimitiveType.STRING));
                add(new Column("col3", PrimitiveType.DECIMAL32));
                add(new Column("col4", PrimitiveType.CHAR));
            }
        };

        mockTargetTable(schema, new HashSet<>());
        String unPartTargetTable = "unpart_" + insertTable;
        String insertSql = "INSERT INTO " + unPartTargetTable + " values(1, 'v1', 32.1, 'aabb')";
        PhysicalPlan physicalSink = getPhysicalPlan(insertSql, PhysicalProperties.SINK_RANDOM_PARTITIONED,
                false);
        checkUnpartTableSinkPlan(schema, unPartTargetTable, physicalSink);

        String insertOverwriteSql = "INSERT OVERWRITE TABLE " + unPartTargetTable + " values(1, 'v1', 32.1, 'aabb')";
        PhysicalPlan physicalOverwriteSink = getPhysicalPlan(insertOverwriteSql, PhysicalProperties.SINK_RANDOM_PARTITIONED,
                true);
        checkUnpartTableSinkPlan(schema, unPartTargetTable, physicalOverwriteSink);

        // test partitioned table
        schema = new ArrayList<Column>() {
            {
                add(new Column("col1", PrimitiveType.INT));
                add(new Column("pt1", PrimitiveType.VARCHAR));
                add(new Column("pt2", PrimitiveType.STRING));
                add(new Column("pt3", PrimitiveType.DATE));
            }
        };
        Set<String> parts = new HashSet<String>() {
            {
                add("pt1");
                add("pt2");
                add("pt3");
            }
        };
        mockTargetTable(schema, parts);
        String partTargetTable = "part_" + insertTable;

        String insertSql2 = "INSERT INTO " + partTargetTable + " values(1, 'v1', 'v2', '2020-03-13')";
        PhysicalPlan physicalSink2 = getPhysicalPlan(insertSql2,
                new PhysicalProperties(new DistributionSpecTableSinkHashPartitioned()), false);
        checkPartTableSinkPlan(schema, partTargetTable, physicalSink2);

        String insertOverwrite2 = "INSERT OVERWRITE TABLE " + partTargetTable + " values(1, 'v1', 'v2', '2020-03-13')";
        PhysicalPlan physicalOverwriteSink2 = getPhysicalPlan(insertOverwrite2,
                new PhysicalProperties(new DistributionSpecTableSinkHashPartitioned()), true);
        checkPartTableSinkPlan(schema, partTargetTable, physicalOverwriteSink2);
    }

    private static void checkUnpartTableSinkPlan(List<Column> schema, String unPartTargetTable, PhysicalPlan physicalSink) {
        Assertions.assertSame(physicalSink.getType(), PlanType.PHYSICAL_DISTRIBUTE);
        // check exchange
        PhysicalDistribute<?> distribute = (PhysicalDistribute<?>) physicalSink;
        Assertions.assertTrue(distribute.getDistributionSpec() instanceof DistributionSpecTableSinkRandomPartitioned);
        Assertions.assertSame(distribute.child(0).getType(), PlanType.PHYSICAL_HIVE_TABLE_SINK);
        // check sink
        PhysicalHiveTableSink<?> physicalHiveSink = (PhysicalHiveTableSink<?>) physicalSink.child(0);
        Assertions.assertEquals(unPartTargetTable, physicalHiveSink.getTargetTable().getName());
        Assertions.assertEquals(schema.size(), physicalHiveSink.getOutput().size());
    }

    private static void checkPartTableSinkPlan(List<Column> schema, String unPartTargetTable, PhysicalPlan physicalSink) {
        Assertions.assertSame(physicalSink.getType(), PlanType.PHYSICAL_DISTRIBUTE);
        // check exchange
        PhysicalDistribute<?> distribute2 = (PhysicalDistribute<?>) physicalSink;
        Assertions.assertTrue(distribute2.getDistributionSpec() instanceof DistributionSpecTableSinkHashPartitioned);
        Assertions.assertSame(distribute2.child(0).getType(), PlanType.PHYSICAL_HIVE_TABLE_SINK);
        // check sink
        PhysicalHiveTableSink<?> physicalHiveSink2 = (PhysicalHiveTableSink<?>) physicalSink.child(0);
        Assertions.assertEquals(unPartTargetTable, physicalHiveSink2.getTargetTable().getName());
        Assertions.assertEquals(schema.size(), physicalHiveSink2.getOutput().size());
    }

    private void createTargetTable(String tableName) throws Exception {
        String createInsertTable = "CREATE TABLE `unpart_" + tableName + "`(\n"
                + "  `col1` INT COMMENT 'col1',\n"
                + "  `col2` STRING COMMENT 'col2',\n"
                + "  `col3` DECIMAL(3,1) COMMENT 'col3',\n"
                + "  `col4` CHAR(11) COMMENT 'col4'\n"
                + ")  ENGINE=hive\n"
                + "PROPERTIES ('file_format'='orc')";
        createTable(createInsertTable, true);

        String createInsertPTable = "CREATE TABLE `part_" + tableName + "`(\n"
                + "  `col1` INT COMMENT 'col1',\n"
                + "  `pt1` VARCHAR(16) COMMENT 'pt1',\n"
                + "  `pt2` STRING COMMENT 'pt2',\n"
                + "  `pt3` DATE COMMENT 'pt3'\n"
                + ")  ENGINE=hive\n"
                + "PARTITION BY LIST (pt1, pt2, pt3) ()\n"
                + "PROPERTIES ('file_format'='orc')";
        createTable(createInsertPTable, true);
    }

    private PhysicalPlan getPhysicalPlan(String insertSql, PhysicalProperties physicalProperties,
                                         boolean isOverwrite) {
        LogicalPlan plan = nereidsParser.parseSingle(insertSql);
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, insertSql);
        Plan exPlan;
        if (isOverwrite) {
            Assertions.assertTrue(plan instanceof InsertOverwriteTableCommand);
            exPlan = ((InsertOverwriteTableCommand) plan).getExplainPlan(connectContext);
        } else {
            Assertions.assertTrue(plan instanceof InsertIntoTableCommand);
            exPlan = ((InsertIntoTableCommand) plan).getExplainPlan(connectContext);
        }
        Assertions.assertTrue(exPlan instanceof UnboundLogicalSink);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        return planner.planWithLock((UnboundLogicalSink<?>) exPlan, physicalProperties);
    }

    @Test
    public void testComplexTypeCreateTable() throws Exception {
        checkedHiveCols = new ArrayList<>(); // init it to enable check
        switchHive();
        useDatabase(mockedDbName);
        String createArrayTypeTable = "CREATE TABLE complex_type_array(\n"
                + "  `col1` ARRAY<BOOLEAN> COMMENT 'col1',\n"
                + "  `col2` ARRAY<INT(11)> COMMENT 'col2',\n"
                + "  `col3` ARRAY<DECIMAL(6,4)> COMMENT 'col3',\n"
                + "  `col4` ARRAY<CHAR(11)> COMMENT 'col4',\n"
                + "  `col5` ARRAY<CHAR> COMMENT 'col5'\n"
                + ")  ENGINE=hive\n"
                + "PROPERTIES ('file_format'='orc')";
        List<FieldSchema> checkArrayCols = new ArrayList<>();
        checkArrayCols.add(new FieldSchema("col1", "array<boolean>", ""));
        checkArrayCols.add(new FieldSchema("col2", "array<int>", ""));
        checkArrayCols.add(new FieldSchema("col3", "array<decimal(6,4)>", ""));
        checkArrayCols.add(new FieldSchema("col4", "array<char(11)>", ""));
        checkArrayCols.add(new FieldSchema("col5", "array<char(1)>", ""));
        resetCheckedColumns(checkArrayCols);

        LogicalPlan plan = createTablesAndReturnPlans(true, createArrayTypeTable).get(0);
        List<Column> columns = ((CreateTableCommand) plan).getCreateTableInfo().translateToLegacyStmt().getColumns();
        Assertions.assertEquals(5, columns.size());
        dropTable("complex_type_array", true);

        String createMapTypeTable = "CREATE TABLE complex_type_map(\n"
                + "  `col1` MAP<int,string> COMMENT 'col1',\n"
                + "  `col2` MAP<string,double> COMMENT 'col2',\n"
                + "  `col3` MAP<string,BOOLEAN> COMMENT 'col3',\n"
                + "  `col4` MAP<BOOLEAN,BOOLEAN> COMMENT 'col4'\n"
                + ")  ENGINE=hive\n"
                + "PROPERTIES ('file_format'='orc')";
        checkArrayCols = new ArrayList<>();
        checkArrayCols.add(new FieldSchema("col1", "map<int,string>", ""));
        checkArrayCols.add(new FieldSchema("col2", "map<string,double>", ""));
        checkArrayCols.add(new FieldSchema("col3", "map<string,boolean>", ""));
        checkArrayCols.add(new FieldSchema("col4", "map<boolean,boolean>", ""));
        resetCheckedColumns(checkArrayCols);

        plan = createTablesAndReturnPlans(true, createMapTypeTable).get(0);
        columns = ((CreateTableCommand) plan).getCreateTableInfo().translateToLegacyStmt().getColumns();
        Assertions.assertEquals(4, columns.size());
        dropTable("complex_type_map", true);

        String createStructTypeTable = "CREATE TABLE complex_type_struct(\n"
                + "  `col1` STRUCT<rates:ARRAY<double>,name:string> COMMENT 'col1',\n"
                + "  `col2` STRUCT<id:INT,age:TINYINT> COMMENT 'col2',\n"
                + "  `col3` STRUCT<pre:DECIMAL(6,4)> COMMENT 'col3',\n"
                + "  `col4` STRUCT<bul:BOOLEAN,buls:ARRAY<BOOLEAN>> COMMENT 'col4'\n"
                + ")  ENGINE=hive\n"
                + "PROPERTIES ('file_format'='orc')";
        checkArrayCols = new ArrayList<>();
        checkArrayCols.add(new FieldSchema("col1", "struct<rates:array<double>,name:string>", ""));
        checkArrayCols.add(new FieldSchema("col2", "struct<id:int,age:tinyint>", ""));
        checkArrayCols.add(new FieldSchema("col3", "struct<pre:decimal(6,4)>", ""));
        checkArrayCols.add(new FieldSchema("col4", "struct<bul:boolean,buls:array<boolean>>", ""));
        resetCheckedColumns(checkArrayCols);

        plan = createTablesAndReturnPlans(true, createStructTypeTable).get(0);
        columns = ((CreateTableCommand) plan).getCreateTableInfo().translateToLegacyStmt().getColumns();
        Assertions.assertEquals(4, columns.size());
        dropTable("complex_type_struct", true);

        String compoundTypeTable1 = "CREATE TABLE complex_type_compound1(\n"
                + "  `col1` ARRAY<MAP<string,double>> COMMENT 'col1',\n"
                + "  `col2` ARRAY<STRUCT<name:string,gender:boolean,rate:decimal(3,1)>> COMMENT 'col2'\n"
                + ")  ENGINE=hive\n"
                + "PROPERTIES ('file_format'='orc')";
        checkArrayCols = new ArrayList<>();
        checkArrayCols.add(new FieldSchema("col1", "array<map<string,double>>", ""));
        checkArrayCols.add(new FieldSchema("col2",
                "array<struct<name:string,gender:boolean,rate:decimal(3,1)>>", ""));
        resetCheckedColumns(checkArrayCols);

        plan = createTablesAndReturnPlans(true, compoundTypeTable1).get(0);
        columns = ((CreateTableCommand) plan).getCreateTableInfo().translateToLegacyStmt().getColumns();
        Assertions.assertEquals(2, columns.size());
        dropTable("complex_type_compound1", true);

        String compoundTypeTable2 = "CREATE TABLE complex_type_compound2(\n"
                + "  `col1` MAP<string,ARRAY<double>> COMMENT 'col1',\n"
                + "  `col2` MAP<string,ARRAY<MAP<int, string>>> COMMENT 'col2',\n"
                + "  `col3` MAP<string,MAP<int,double>> COMMENT 'col3',\n"
                + "  `col4` MAP<bigint,STRUCT<name:string,gender:boolean,rate:decimal(3,1)>> COMMENT 'col4'\n"
                + ")  ENGINE=hive\n"
                + "PROPERTIES ('file_format'='orc')";
        checkArrayCols = new ArrayList<>();
        checkArrayCols.add(new FieldSchema("col1", "map<string,array<double>>", ""));
        checkArrayCols.add(new FieldSchema("col2", "map<string,array<map<int,string>>>", ""));
        checkArrayCols.add(new FieldSchema("col3", "map<string,map<int,double>>", ""));
        checkArrayCols.add(new FieldSchema("col4",
                "map<bigint,struct<name:string,gender:boolean,rate:decimal(3,1)>>", ""));
        resetCheckedColumns(checkArrayCols);

        plan = createTablesAndReturnPlans(true, compoundTypeTable2).get(0);
        columns = ((CreateTableCommand) plan).getCreateTableInfo().translateToLegacyStmt().getColumns();
        Assertions.assertEquals(4, columns.size());
        dropTable("complex_type_compound2", true);
    }

    private void resetCheckedColumns(List<FieldSchema> checkArrayCols) {
        checkedHiveCols.clear();
        checkedHiveCols.addAll(checkArrayCols);
    }
}
