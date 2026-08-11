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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.alter.AlterOperations;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.PropertyAnalyzer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class ModifyPartitionOpTest {

    @Test
    public void rowBinlogTableRejectsStorageRelocationProperties() {
        ModifyPartitionOp storageMedium = new ModifyPartitionOp(Lists.newArrayList("p0"),
                ImmutableMap.of("STORAGE_MEDIUM", "SSD"), false);
        ModifyPartitionOp cooldownTime = ModifyPartitionOp.createStarClause(
                ImmutableMap.of("Storage_Cooldown_Time", "2026-01-01 00:00:00"), false);

        Assert.assertFalse(storageMedium.allowOpRowBinlog());
        Assert.assertFalse(cooldownTime.allowOpRowBinlog());
        AlterOperations alterOperations = new AlterOperations();
        Assert.assertThrows(DdlException.class,
                () -> alterOperations.checkRowBinlogAllow(Lists.newArrayList(storageMedium)));
        Assert.assertThrows(DdlException.class,
                () -> alterOperations.checkRowBinlogAllow(Lists.newArrayList(cooldownTime)));
    }

    @Test
    public void rowBinlogTableStillAllowsReplicationModification() throws DdlException {
        ModifyPartitionOp replication = new ModifyPartitionOp(Lists.newArrayList("p0"),
                ImmutableMap.of(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "3"), false);

        Assert.assertTrue(replication.allowOpRowBinlog());
        new AlterOperations().checkRowBinlogAllow(Lists.newArrayList(replication));
    }

    @Test
    public void rowBinlogTableRejectsTableLevelStorageRelocation() {
        ModifyTablePropertiesOp storageMedium = new ModifyTablePropertiesOp(
                ImmutableMap.of(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD"));
        ModifyTablePropertiesOp upperCaseStorageMedium = new ModifyTablePropertiesOp(
                ImmutableMap.of("STORAGE_MEDIUM", "SSD"));
        ModifyTablePropertiesOp cooldownTime = new ModifyTablePropertiesOp(
                ImmutableMap.of("Storage_Cooldown_Time", "2026-01-01 00:00:00"));

        Assert.assertFalse(storageMedium.allowOpRowBinlog());
        Assert.assertFalse(upperCaseStorageMedium.allowOpRowBinlog());
        Assert.assertFalse(cooldownTime.allowOpRowBinlog());
        AlterOperations alterOperations = new AlterOperations();
        Assert.assertThrows(DdlException.class,
                () -> alterOperations.checkRowBinlogAllow(Lists.newArrayList(storageMedium)));
    }

    @Test
    public void rowBinlogTableStillAllowsTableLevelReplicationModification() throws DdlException {
        ModifyTablePropertiesOp replication = new ModifyTablePropertiesOp(
                ImmutableMap.of(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "3"));

        Assert.assertTrue(replication.allowOpRowBinlog());
        new AlterOperations().checkRowBinlogAllow(Lists.newArrayList(replication));
    }
}
