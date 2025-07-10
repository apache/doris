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

import org.apache.doris.common.DdlException;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class CreateTableWithStorageMediumSpecifiedTest extends TestWithFeService {

    @Override
    protected void runAfterAll() throws Exception {
        Env.getCurrentEnv().clear();
    }

    @Test
    public void testCreateTableWithStorageMediumSpecifiedTrue() throws Exception {
        // Clean up any existing database
        try {
            dropDatabase("test_db");
        } catch (Exception e) {
            // Ignore if database doesn't exist
        }
        createDatabase("test_db");

        // Test with storage_medium_specified = true
        String sql = "CREATE TABLE IF NOT EXISTS test_db.t1 (pk INT, v1 INT sum) AGGREGATE KEY (pk) "
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1', 'storage_medium_specified' = 'true');";
        
        Assertions.assertDoesNotThrow(() -> createTables(sql));
        
        // Verify the table was created successfully
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test_db");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("t1");
        Assertions.assertNotNull(table);
        
        // Check that the property was set correctly
        Set<String> partitionNames = table.getPartitionNames();
        Assertions.assertFalse(partitionNames.isEmpty());
        String firstPartitionName = partitionNames.iterator().next();
        Partition partition = table.getPartition(firstPartitionName);
        Assertions.assertNotNull(partition);
        DataProperty dataProperty = table.getPartitionInfo().getDataProperty(partition.getId());
        Assertions.assertTrue(dataProperty.isStorageMediumSpecified());
    }

    @Test
    public void testCreateTableWithStorageMediumSpecifiedFalse() throws Exception {
        // Clean up any existing database
        try {
            dropDatabase("test_db");
        } catch (Exception e) {
            // Ignore if database doesn't exist
        }
        createDatabase("test_db");

        // Test with storage_medium_specified = false
        String sql = "CREATE TABLE IF NOT EXISTS test_db.t2 (pk INT, v1 INT sum) AGGREGATE KEY (pk) "
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1', 'storage_medium_specified' = 'false');";
        
        Assertions.assertDoesNotThrow(() -> createTables(sql));
        
        // Verify the table was created successfully
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test_db");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("t2");
        Assertions.assertNotNull(table);
        
        // Check that the property was set correctly
        Set<String> partitionNames = table.getPartitionNames();
        Assertions.assertFalse(partitionNames.isEmpty());
        String firstPartitionName = partitionNames.iterator().next();
        Partition partition = table.getPartition(firstPartitionName);
        Assertions.assertNotNull(partition);
        DataProperty dataProperty = table.getPartitionInfo().getDataProperty(partition.getId());
        Assertions.assertFalse(dataProperty.isStorageMediumSpecified());
    }

    @Test
    public void testCreateTableWithStorageMediumAndStorageMediumSpecified() throws Exception {
        // Clean up any existing database
        try {
            dropDatabase("test_db");
        } catch (Exception e) {
            // Ignore if database doesn't exist
        }
        createDatabase("test_db");

        // Test with both storage_medium and storage_medium_specified
        String sql = "CREATE TABLE IF NOT EXISTS test_db.t3 (pk INT, v1 INT sum) AGGREGATE KEY (pk) "
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1', 'storage_medium' = 'HDD', 'storage_medium_specified' = 'true');";
        
        Assertions.assertDoesNotThrow(() -> createTables(sql));
        
        // Verify the table was created successfully
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test_db");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("t3");
        Assertions.assertNotNull(table);
        
        // Check that both properties were set correctly
        Set<String> partitionNames = table.getPartitionNames();
        Assertions.assertFalse(partitionNames.isEmpty());
        String firstPartitionName = partitionNames.iterator().next();
        Partition partition = table.getPartition(firstPartitionName);
        Assertions.assertNotNull(partition);
        DataProperty dataProperty = table.getPartitionInfo().getDataProperty(partition.getId());
        Assertions.assertEquals(TStorageMedium.HDD, dataProperty.getStorageMedium());
        Assertions.assertTrue(dataProperty.isStorageMediumSpecified());
    }

    @Test
    public void testCreateTableWithInvalidStorageMediumSpecified() throws Exception {
        // Clean up any existing database
        try {
            dropDatabase("test_db");
        } catch (Exception e) {
            // Ignore if database doesn't exist
        }
        createDatabase("test_db");

        // Test with invalid storage_medium_specified value
        String sql = "CREATE TABLE IF NOT EXISTS test_db.t4 (pk INT, v1 INT sum) AGGREGATE KEY (pk) "
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1', 'storage_medium_specified' = 'invalid');";
        
        Assertions.assertThrows(DdlException.class, () -> createTables(sql));
    }
} 