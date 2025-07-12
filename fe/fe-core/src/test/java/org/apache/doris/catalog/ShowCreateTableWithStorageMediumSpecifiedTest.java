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

package org.apache.doris.catalog;

import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.ShowCreateTableStmt;
import org.apache.doris.analysis.ShowPartitionsStmt;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowExecutor;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.TestWithFeService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class ShowCreateTableWithStorageMediumSpecifiedTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("test");
        useDatabase("test");
    }

    @Test
    public void testShowCreateTableWithStorageMediumSpecified() throws Exception {
        // Create table with storage_medium_specified = true
        String createTableSql = "CREATE TABLE test_storage_medium_specified (\n"
                + "    k1 INT,\n"
                + "    k2 VARCHAR(20)\n"
                + ") DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "    'replication_num' = '1',\n"
                + "    'storage_medium' = 'HDD',\n"
                + "    'storage_medium_specified' = 'true'\n"
                + ");";
        
        CreateTableStmt createTableStmt = (CreateTableStmt) parseAndAnalyzeStmt(createTableSql);
        Env.getCurrentEnv().createTable(createTableStmt);
        
        // Execute SHOW CREATE TABLE
        String showCreateTableSql = "SHOW CREATE TABLE test_storage_medium_specified";
        ShowCreateTableStmt showCreateTableStmt = (ShowCreateTableStmt) parseAndAnalyzeStmt(showCreateTableSql);
        ShowExecutor executor = new ShowExecutor(connectContext, showCreateTableStmt);
        ShowResultSet resultSet = executor.execute();
        
        // Verify result contains storage_medium_specified property
        String createTableDdl = resultSet.getResultRows().get(0).get(1);
        Assertions.assertTrue(createTableDdl.contains("storage_medium_specified"),
                "SHOW CREATE TABLE should contain storage_medium_specified property");
        
        // Verify table properties actually set storage_medium_specified
        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("test_storage_medium_specified");
        Map<String, String> properties = table.getTableProperty().getProperties();
        
        Assertions.assertTrue(properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM_SPECIFIED),
                "Table should have storage_medium_specified property");
        Assertions.assertEquals("true", properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM_SPECIFIED),
                "storage_medium_specified should be 'true'");
    }

    @Test
    public void testShowCreateTableWithoutStorageMediumSpecified() throws Exception {
        // Create table without storage_medium_specified
        String createTableSql = "CREATE TABLE test_no_storage_medium_specified (\n"
                + "    k1 INT,\n"
                + "    k2 VARCHAR(20)\n"
                + ") DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "    'replication_num' = '1'\n"
                + ");";
        
        CreateTableStmt createTableStmt = (CreateTableStmt) parseAndAnalyzeStmt(createTableSql);
        Env.getCurrentEnv().createTable(createTableStmt);
        
        // Execute SHOW CREATE TABLE
        String showCreateTableSql = "SHOW CREATE TABLE test_no_storage_medium_specified";
        ShowCreateTableStmt showCreateTableStmt = (ShowCreateTableStmt) parseAndAnalyzeStmt(showCreateTableSql);
        ShowExecutor executor = new ShowExecutor(connectContext, showCreateTableStmt);
        ShowResultSet resultSet = executor.execute();
        
        // Verify result contains storage_medium_specified property with default value
        String createTableDdl = resultSet.getResultRows().get(0).get(1);
        Assertions.assertTrue(createTableDdl.contains("storage_medium_specified"),
                "SHOW CREATE TABLE should contain storage_medium_specified property false");
        
        // Verify table properties have storage_medium_specified property with default value
        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("test_no_storage_medium_specified");
        Map<String, String> properties = table.getTableProperty().getProperties();
        
        Assertions.assertEquals("false", properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM_SPECIFIED),
                "Table should have storage_medium_specified property false");
    }

    @Test
    public void testShowPartitionsShowsStorageMediumSpecified() throws Exception {
        // Create partitioned table with storage_medium_specified = true
        String createTableSql = "CREATE TABLE test_partition_storage_medium_specified (\n"
                + "    k1 INT,\n"
                + "    k2 VARCHAR(20),\n"
                + "    dt DATE\n"
                + ") DUPLICATE KEY(k1)\n"
                + "PARTITION BY RANGE(dt)\n"
                + "(\n"
                + "    PARTITION p1 VALUES LESS THAN ('2023-01-01'),\n"
                + "    PARTITION p2 VALUES LESS THAN ('2023-02-01')\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "    'replication_num' = '1',\n"
                + "    'storage_medium' = 'HDD',\n"
                + "    'storage_medium_specified' = 'true'\n"
                + ");";
        
        CreateTableStmt createTableStmt = (CreateTableStmt) parseAndAnalyzeStmt(createTableSql);
        Env.getCurrentEnv().createTable(createTableStmt);
        
        // Execute SHOW PARTITIONS
        String showPartitionsSql = "SHOW PARTITIONS FROM test_partition_storage_medium_specified";
        ShowPartitionsStmt showPartitionsStmt = (ShowPartitionsStmt) parseAndAnalyzeStmt(showPartitionsSql);
        ShowExecutor executor = new ShowExecutor(connectContext, showPartitionsStmt);
        ShowResultSet resultSet = executor.execute();
        
        // Verify result contains StorageMediumSpecified column
        Assertions.assertTrue(resultSet.getMetaData().getColumns().stream()
                        .anyMatch(col -> col.getName().equals("StorageMediumSpecified")),
                "SHOW PARTITIONS should have StorageMediumSpecified column");
        
        // Verify result contains StorageMedium column
        Assertions.assertTrue(resultSet.getMetaData().getColumns().stream()
                        .anyMatch(col -> col.getName().equals("StorageMedium")),
                "SHOW PARTITIONS should have StorageMedium column");
    }
} 