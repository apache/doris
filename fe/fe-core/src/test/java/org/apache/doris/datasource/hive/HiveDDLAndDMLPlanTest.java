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
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.analysis.SwitchStmt;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.TableMetadata;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class HiveDDLAndDMLPlanTest extends TestWithFeService {
    private static final String mockedDbName = "mockedDb";
    private final NereidsParser nereidsParser = new NereidsParser();

    @Mocked
    private ThriftHMSCachedClient mockedHiveClient;

    @Override
    protected void runBeforeAll() throws Exception {
        connectContext.getSessionVariable().enableFallbackToOriginalPlanner = false;
        connectContext.getSessionVariable().enableNereidsTimeout = false;
        connectContext.getSessionVariable().enableNereidsDML = true;
        Config.enable_query_hive_views = false;
        Config.enable_external_ddl = true;
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
                + "  `pt1` VARCHAR COMMENT 'pt1',\n"
                + "  `pt2` VARCHAR COMMENT 'pt2'\n"
                + ")  ENGINE=olap\n"
                + "PARTITION BY LIST (pt1, pt2) ()\n"
                + "DISTRIBUTED BY HASH (col1) BUCKETS 16\n"
                + "PROPERTIES (\n"
                + "  'replication_num' = '1')";
        createTable(createSourceInterPTable, true);

        // create external catalog and switch it
        CreateCatalogStmt hiveCatalog = createStmt("create catalog hive properties('type' = 'hms',"
                        + " 'hive.metastore.uris' = 'thrift://192.168.0.1:9083');");
        Env.getCurrentEnv().getCatalogMgr().createCatalog(hiveCatalog);
        switchHive();

        // create db and use it
        Map<String, String> dbProps = new HashMap<>();
        dbProps.put(HiveMetadataOps.LOCATION_URI_KEY, "file://loc/db");
        new MockUp<ThriftHMSCachedClient>(ThriftHMSCachedClient.class) {
            @Mock
            public List<String> getAllDatabases() {
                return new ArrayList<String>() {
                    {
                        add(mockedDbName);
                    }
                };
            }

            @Mock
            public void createTable(TableMetadata tbl, boolean ignoreIfExists) {
                if (tbl instanceof HiveTableMetadata) {
                    ThriftHMSCachedClient.toHiveTable((HiveTableMetadata) tbl);
                }
            }
        };
        CreateDbStmt createDbStmt = new CreateDbStmt(true, mockedDbName, dbProps);
        Env.getCurrentEnv().createDb(createDbStmt);
        useDatabase(mockedDbName);

        // un-partitioned table
        String createSourceExtUTable = "CREATE TABLE `unpart_ctas_src`(\n"
                + "  `col1` INT COMMENT 'col1',\n"
                + "  `col2` STRING COMMENT 'col2'\n"
                + ")  ENGINE=hive\n"
                + "PROPERTIES (\n"
                + "  'location_uri'='hdfs://loc/db/tbl',\n"
                + "  'file_format'='parquet')";
        createTable(createSourceExtUTable, true);
        // partitioned table
        String createSourceExtTable = "CREATE TABLE `part_ctas_src`(\n"
                + "  `col1` INT COMMENT 'col1',\n"
                + "  `pt1` STRING COMMENT 'pt1',\n"
                + "  `pt2` STRING COMMENT 'pt2'\n"
                + ")  ENGINE=hive\n"
                + "PARTITION BY LIST (pt1, pt2) ()\n"
                + "PROPERTIES (\n"
                + "  'location_uri'='hdfs://loc/db/tbl',\n"
                + "  'file_format'='orc')";
        createTable(createSourceExtTable, true);

        HMSExternalCatalog hmsExternalCatalog = (HMSExternalCatalog) Env.getCurrentEnv().getCatalogMgr()
                .getCatalog("hive");
        new MockUp<HMSExternalDatabase>(HMSExternalDatabase.class) {
            @Mock
            HMSExternalTable getTableNullable(String tableName) {
                return new HMSExternalTable(0, tableName, mockedDbName, hmsExternalCatalog);
            }
        };
        new MockUp<HMSExternalTable>(HMSExternalTable.class) {
            @Mock
            protected synchronized void makeSureInitialized() {
                // mocked
            }
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
        dropDatabase(mockedDbName);
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
                + "  'location_uri'='hdfs://loc/db/tbl',\n"
                + "  'file_format'='orc')";
        createTable(createUnPartTable, true);
        dropTable("unpart_tbl", true);

        String createPartTable = "CREATE TABLE `part_tbl`(\n"
                + "  `col1` BOOLEAN COMMENT 'col1',\n"
                + "  `col2` INT COMMENT 'col2',\n"
                + "  `col3` BIGINT COMMENT 'col3',\n"
                + "  `col4` DECIMAL(5,2) COMMENT 'col4',\n"
                + "  `pt1` STRING COMMENT 'pt1',\n"
                + "  `pt2` STRING COMMENT 'pt2',\n"
                + "  `col5` DATE COMMENT 'col5',\n"
                + "  `col6` DATETIME COMMENT 'col6'\n"
                + ")  ENGINE=hive\n"
                + "PARTITION BY LIST (pt1, pt2) ()\n"
                + "PROPERTIES (\n"
                + "  'location_uri'='hdfs://loc/db/tbl',\n"
                + "  'file_format'='parquet')";
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
                + "  'location_uri'='hdfs://loc/db/tbl',\n"
                + "  'file_format'='orc')";
        ExceptionChecker.expectThrowsWithMsg(org.apache.doris.nereids.exceptions.AnalysisException.class,
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
                + "  'location_uri'='hdfs://loc/db/tbl',\n"
                + "  'file_format'='orc')";
        createTable(createBucketedTableOk1, true);
        dropTable("buck_tbl", true);

        String createBucketedTableOk2 = "CREATE TABLE `part_buck_tbl`(\n"
                + "  `col1` BOOLEAN COMMENT 'col1',\n"
                + "  `col2` INT COMMENT 'col2',\n"
                + "  `col3` BIGINT COMMENT 'col3',\n"
                + "  `col4` DECIMAL(5,2) COMMENT 'col4',\n"
                + "  `pt1` STRING COMMENT 'pt1',\n"
                + "  `pt2` STRING COMMENT 'pt2'\n"
                + ")  ENGINE=hive\n"
                + "PARTITION BY LIST (pt2) ()\n"
                + "DISTRIBUTED BY HASH (col2) BUCKETS 16\n"
                + "PROPERTIES (\n"
                + "  'location_uri'='hdfs://loc/db/tbl',\n"
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
        String olapCtasErr = "CREATE TABLE no_buck_olap AS SELECT * FROM internal.mockedDb.olap_src";
        LogicalPlan olapCtasErrPlan = nereidsParser.parseSingle(olapCtasErr);
        Assertions.assertTrue(olapCtasErrPlan instanceof CreateTableCommand);
        ExceptionChecker.expectThrowsWithMsg(org.apache.doris.nereids.exceptions.AnalysisException.class,
                "Cannot create olap table out of internal catalog."
                        + "Make sure 'engine' type is specified when use the catalog: hive",
                () -> ((CreateTableCommand) olapCtasErrPlan).run(connectContext, null));

        String olapCtasOk = "CREATE TABLE internal.mockedDb.no_buck_olap"
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

    @Test
    public void testInsertIntoPlanSql() throws Exception {
        switchHive();
        useDatabase(mockedDbName);
        String insertSql = "INSERT INTO unpart_ctas_src values(1, 'v1')";
        LogicalPlan plan = nereidsParser.parseSingle(insertSql);
        Assertions.assertTrue(plan instanceof InsertIntoTableCommand);
        // TODO check plan node, exchange node

        String insertSql2 = "INSERT INTO part_ctas_src values(1, 'v1', 'v2')";
        LogicalPlan plan2 = nereidsParser.parseSingle(insertSql2);
        Assertions.assertTrue(plan2 instanceof InsertIntoTableCommand);
    }

    @Test
    public void testInsertOverwritePlanSql() throws Exception {
        switchHive();
        useDatabase(mockedDbName);
        String insertSql = "INSERT OVERWRITE TABLE unpart_ctas_src values(2, 'v2')";
        LogicalPlan plan = nereidsParser.parseSingle(insertSql);
        Assertions.assertTrue(plan instanceof InsertOverwriteTableCommand);
        // TODO check plan node, exchange node

        String insertSql2 = "INSERT OVERWRITE TABLE part_ctas_src values(2, 'v3', 'v4')";
        LogicalPlan plan2 = nereidsParser.parseSingle(insertSql2);
        Assertions.assertTrue(plan2 instanceof InsertOverwriteTableCommand);
    }

    @Test
    public void testComplexTypeCreateTable() throws Exception {
        switchHive();
        useDatabase(mockedDbName);
        String createArrayTypeTable = "CREATE TABLE complex_type_array(\n"
                + "  `col1` ARRAY<BOOLEAN> COMMENT 'col1',\n"
                + "  `col2` ARRAY<INT(11)> COMMENT 'col2',\n"
                + "  `col3` ARRAY<DECIMAL(6,4)> COMMENT 'col3'\n"
                + ")  ENGINE=hive\n"
                + "PROPERTIES ('file_format'='orc')";
        createTable(createArrayTypeTable, true);
        dropTable("complex_type_array", true);

        String createMapTypeTable = "CREATE TABLE complex_type_map(\n"
                + "  `col1` MAP<int,string> COMMENT 'col1',\n"
                + "  `col2` MAP<string,double> COMMENT 'col2',\n"
                + "  `col3` MAP<string,BOOLEAN> COMMENT 'col3',\n"
                + "  `col4` MAP<BOOLEAN,BOOLEAN> COMMENT 'col4'\n"
                + ")  ENGINE=hive\n"
                + "PROPERTIES ('file_format'='orc')";
        createTable(createMapTypeTable, true);
        dropTable("complex_type_map", true);

        String createStructTypeTable = "CREATE TABLE complex_type_struct(\n"
                + "  `col1` STRUCT<rates:ARRAY<double>,name:string> COMMENT 'col1',\n"
                + "  `col2` STRUCT<id:INT,age:TINYINT> COMMENT 'col2',\n"
                + "  `col3` STRUCT<pre:DECIMAL(6,4)> COMMENT 'col3',\n"
                + "  `col4` STRUCT<bul:BOOLEAN,buls:ARRAY<BOOLEAN>> COMMENT 'col4'\n"
                + ")  ENGINE=hive\n"
                + "PROPERTIES ('file_format'='orc')";
        createTable(createStructTypeTable, true);
        dropTable("complex_type_struct", true);

        String compoundTypeTable1 = "CREATE TABLE complex_type_compound1(\n"
                + "  `col1` ARRAY<MAP<string,double>> COMMENT 'col1',\n"
                + "  `col2` ARRAY<STRUCT<name:string,gender:boolean,rate:decimal(3,1)>> COMMENT 'col2'\n"
                + ")  ENGINE=hive\n"
                + "PROPERTIES ('file_format'='orc')";
        createTable(compoundTypeTable1, true);
        dropTable("complex_type_compound1", true);

        String compoundTypeTable2 = "CREATE TABLE complex_type_compound2(\n"
                + "  `col1` MAP<string,ARRAY<double>> COMMENT 'col1',\n"
                + "  `col2` MAP<string,ARRAY<MAP<int, string>>> COMMENT 'col2',\n"
                + "  `col3` MAP<string,MAP<int,double>> COMMENT 'col3',\n"
                + "  `col4` MAP<bigint,STRUCT<name:string,gender:boolean,rate:decimal(3,1)>> COMMENT 'col4'\n"
                + ")  ENGINE=hive\n"
                + "PROPERTIES ('file_format'='orc')";
        createTable(compoundTypeTable2, true);
        dropTable("complex_type_compound2", true);
    }
}
