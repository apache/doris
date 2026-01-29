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

package org.apache.doris.datasource.hive.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.SessionVariable;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

public class HiveScanNodeTest {
    private static final long MB = 1024L * 1024L;

    @Test
    public void testDetermineTargetFileSplitSizeHonorsMaxFileSplitNum() throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(100);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.bindBrokerName()).thenReturn("");
        desc.setTable(table);
        HiveScanNode node = new HiveScanNode(new PlanNodeId(0), desc, false, sv, null);

        HiveMetaStoreCache.FileCacheValue fileCacheValue = new HiveMetaStoreCache.FileCacheValue();
        HiveMetaStoreCache.HiveFileStatus status = new HiveMetaStoreCache.HiveFileStatus();
        status.setLength(10_000L * MB);
        fileCacheValue.getFiles().add(status);
        List<HiveMetaStoreCache.FileCacheValue> caches = Collections.singletonList(fileCacheValue);

        Method method = HiveScanNode.class.getDeclaredMethod(
                "determineTargetFileSplitSize", List.class, boolean.class);
        method.setAccessible(true);
        long target = (long) method.invoke(node, caches, false);
        Assert.assertEquals(100 * MB, target);
    }

    @Test
    public void testDetermineTargetFileSplitSizeKeepsInitialSize() throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(100);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.bindBrokerName()).thenReturn("");
        desc.setTable(table);
        HiveScanNode node = new HiveScanNode(new PlanNodeId(0), desc, false, sv, null);

        HiveMetaStoreCache.FileCacheValue fileCacheValue = new HiveMetaStoreCache.FileCacheValue();
        HiveMetaStoreCache.HiveFileStatus status = new HiveMetaStoreCache.HiveFileStatus();
        status.setLength(500L * MB);
        fileCacheValue.getFiles().add(status);
        List<HiveMetaStoreCache.FileCacheValue> caches = Collections.singletonList(fileCacheValue);

        Method method = HiveScanNode.class.getDeclaredMethod(
                "determineTargetFileSplitSize", List.class, boolean.class);
        method.setAccessible(true);
        long target = (long) method.invoke(node, caches, false);
        Assert.assertEquals(32 * MB, target);
    }
}
