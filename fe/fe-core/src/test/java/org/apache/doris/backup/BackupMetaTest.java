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

package org.apache.doris.backup;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.UnitTestUtil;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;

import java.util.Collection;
import java.util.Map;

public class BackupMetaTest {

    private long dbId = 1;
    private long tblId = 2;
    private long partId = 3;
    private long idxId = 4;
    private long tabletId = 5;
    private long backendId = 10000;
    private long version = 6;

    private Database db;
    private OlapTable originalTable;

    @Before
    public void setUp() {
        // Create test database and table
        db = UnitTestUtil.createDb(dbId, tblId, partId, idxId, tabletId, backendId, version);
        originalTable = (OlapTable) db.getTableNullable(tblId);
        Assert.assertNotNull(originalTable);
    }

    @Test
    public void testBackupMetaPreservesOriginalStorageMedium() {
        // Test case 1: Backup operation should always preserve original SSD medium
        // Set partition to SSD storage medium
        Partition partition = originalTable.getPartition(partId);
        Assert.assertNotNull(partition);
        
        DataProperty ssdProperty = new DataProperty(TStorageMedium.SSD);
        originalTable.getPartitionInfo().setDataProperty(partId, ssdProperty);
        
        // Verify original table has SSD storage medium
        TStorageMedium originalMedium = originalTable.getPartitionInfo().getDataProperty(partId).getStorageMedium();
        Assert.assertEquals(TStorageMedium.SSD, originalMedium);
        
        // Create selective copy for backup (isForBackup=true)
        OlapTable copiedTable = originalTable.selectiveCopy(null, IndexExtState.VISIBLE, true);
        Assert.assertNotNull(copiedTable);
        
        // Verify that the copied table preserves the original SSD medium
        TStorageMedium copiedMedium = copiedTable.getPartitionInfo().getDataProperty(partId).getStorageMedium();
        Assert.assertEquals("Backup operation should always preserve original storage medium", 
                          TStorageMedium.SSD, copiedMedium);
        
        // Test case 2: Verify non-backup operations also preserve medium
        OlapTable copiedTableNonBackup = originalTable.selectiveCopy(null, IndexExtState.VISIBLE, false);
        Assert.assertNotNull(copiedTableNonBackup);
        
        // For non-backup operations, the original medium should be preserved
        TStorageMedium nonBackupMedium = copiedTableNonBackup.getPartitionInfo().getDataProperty(partId).getStorageMedium();
        Assert.assertEquals("Non-backup operations should preserve original medium", 
                          TStorageMedium.SSD, nonBackupMedium);
    }

    @Test
    public void testBackupMetaWithHDDPartition() {
        // Test the default HDD case - backup should always preserve original medium
        // Verify original table has HDD storage medium (default from UnitTestUtil.createDb)
        TStorageMedium originalMedium = originalTable.getPartitionInfo().getDataProperty(partId).getStorageMedium();
        Assert.assertEquals(TStorageMedium.HDD, originalMedium);
        
        // Create selective copy for backup
        OlapTable copiedTable = originalTable.selectiveCopy(null, IndexExtState.VISIBLE, true);
        Assert.assertNotNull(copiedTable);
        
        // Verify that HDD medium is preserved
        TStorageMedium copiedMedium = copiedTable.getPartitionInfo().getDataProperty(partId).getStorageMedium();
        Assert.assertEquals("HDD medium should be preserved during backup", 
                          TStorageMedium.HDD, copiedMedium);
    }
} 