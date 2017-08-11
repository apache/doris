// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.backup;

import com.baidu.palo.analysis.LabelName;
import com.baidu.palo.backup.BackupJob.BackupJobState;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.common.Config;

import com.google.common.collect.Maps;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Map;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("org.apache.log4j.*")
@PrepareForTest(Catalog.class)
public class BackupJobTest {

    private Catalog catalog;

    @Before
    public void setUp() {
        catalog = CatalogMocker.fetchAdminCatalog();

        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        PowerMock.replay(Catalog.class);
    }

    @Test
    public void testSaveMeta() {
        Map<String, String> properties = Maps.newHashMap();
        LabelName labelName = new LabelName(CatalogMocker.TEST_DB_NAME, "test_backup");
        BackupJob backupJob = new BackupJob(1, CatalogMocker.TEST_DB_ID, labelName, "/home/backup/", properties);
        backupJob.setState(BackupJobState.PENDING);

        backupJob.addPartitionId(CatalogMocker.TEST_MYSQL_TABLE_ID, -1);
        backupJob.addPartitionId(CatalogMocker.TEST_TBL_ID, CatalogMocker.TEST_SINGLE_PARTITION_ID);
        backupJob.addPartitionId(CatalogMocker.TEST_TBL2_ID, CatalogMocker.TEST_PARTITION1_ID);
        backupJob.addPartitionId(CatalogMocker.TEST_TBL2_ID, CatalogMocker.TEST_PARTITION2_ID);

        Config.meta_dir = "palo-meta/";
        backupJob.runOnce();
    }
}
