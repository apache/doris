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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class ProcServiceTest {
    private class EmptyProcNode implements ProcNodeInterface {
        @Override
        public ProcResult fetchResult() {
            return null;
        }
    }

    // 构造用于测试的文件结构，具体的结构如下
    // - palo
    // | - be
    //   | - src
    //   | - deps
    // | - fe
    //   | - src
    //   | - conf
    //   | - build.sh
    // | - common
    @Before
    public void beforeTest() {
        ProcService procService = ProcService.getInstance();

        BaseProcDir paloDir = new BaseProcDir();
        Assert.assertTrue(procService.register("palo", paloDir));

        BaseProcDir beDir = new BaseProcDir();
        Assert.assertTrue(paloDir.register("be", beDir));
        Assert.assertTrue(beDir.register("src", new BaseProcDir()));
        Assert.assertTrue(beDir.register("deps", new BaseProcDir()));

        BaseProcDir feDir = new BaseProcDir();
        Assert.assertTrue(paloDir.register("fe", feDir));
        Assert.assertTrue(feDir.register("src", new BaseProcDir()));
        Assert.assertTrue(feDir.register("conf", new BaseProcDir()));
        Assert.assertTrue(feDir.register("build.sh", new EmptyProcNode()));

        Assert.assertTrue(paloDir.register("common", new BaseProcDir()));
    }

    @After
    public void afterTest() {
        ProcService.destroy();
    }

    @Test
    public void testRegisterNormal() {
        ProcService procService = ProcService.getInstance();
        String name = "test";
        BaseProcDir dir = new BaseProcDir();

        Assert.assertTrue(procService.register(name, dir));
    }

    // register second time
    @Test
    public void testRegisterSecond() {
        ProcService procService = ProcService.getInstance();
        String name = "test";
        BaseProcDir dir = new BaseProcDir();

        Assert.assertTrue(procService.register(name, dir));
        Assert.assertFalse(procService.register(name, dir));
    }

    // register invalid
    @Test
    public void testRegisterInvalidInput() {
        ProcService procService = ProcService.getInstance();
        String name = "test";
        BaseProcDir dir = new BaseProcDir();

        Assert.assertFalse(procService.register(null, dir));
        Assert.assertFalse(procService.register("", dir));
        Assert.assertFalse(procService.register(name, null));
    }

    @Test
    public void testOpenNormal() throws AnalysisException {
        ProcService procService = ProcService.getInstance();

        // assert root
        Assert.assertNotNull(procService.open("/"));
        Assert.assertNotNull(procService.open("/palo"));
        Assert.assertNotNull(procService.open("/palo/be"));
        Assert.assertNotNull(procService.open("/palo/be/src"));
        Assert.assertNotNull(procService.open("/palo/be/deps"));
        Assert.assertNotNull(procService.open("/palo/fe"));
        Assert.assertNotNull(procService.open("/palo/fe/src"));
        Assert.assertNotNull(procService.open("/palo/fe/conf"));
        Assert.assertNotNull(procService.open("/palo/fe/build.sh"));
        Assert.assertNotNull(procService.open("/palo/common"));
    }

    @Test
    public void testOpenSapceNormal() throws AnalysisException {
        ProcService procService = ProcService.getInstance();

        // assert space
        Assert.assertNotNull(procService.open(" \r/"));
        Assert.assertNotNull(procService.open(" \r/ "));
        Assert.assertNotNull(procService.open("  /palo \r\n"));
        Assert.assertNotNull(procService.open("\n\r\t /palo/be \n\r"));

        // assert last '/'
        Assert.assertNotNull(procService.open(" /palo/be/"));
        Assert.assertNotNull(procService.open(" /palo/fe/  "));

        ProcNodeInterface node = procService.open("/dbs");
        Assert.assertNotNull(node);
        Assert.assertTrue(node instanceof DbsProcDir);
    }

    @Test
    public void testOpenFail() {
        ProcService procService = ProcService.getInstance();

        // assert no path
        int errCount = 0;
        try {
            procService.open("/abc");
        } catch (AnalysisException e) {
            ++errCount;
        }
        try {
            Assert.assertNull(procService.open("/palo/b e"));
        } catch (AnalysisException e) {
            ++errCount;
        }
        try {
            Assert.assertNull(procService.open("/palo/fe/build.sh/"));
        } catch (AnalysisException e) {
            ++errCount;
        }

        // assert no root
        try {
            Assert.assertNull(procService.open("palo"));
        } catch (AnalysisException e) {
            ++errCount;
        }
        try {
            Assert.assertNull(procService.open(" palo"));
        } catch (AnalysisException e) {
            ++errCount;
        }

        Assert.assertEquals(5, errCount);
    }

    @Test
    public void testTabletProc() throws AnalysisException {
        List<String> replicasTitles = ReplicasProcNode.TITLE_NAMES;
        int replicaIdIdx = replicasTitles.indexOf("ReplicaId");
        int backendIdIdx = replicasTitles.indexOf("BackendId");
        int versionIdx = replicasTitles.indexOf("Version");
        int lastSuccessVersionIdx = replicasTitles.indexOf("LstSuccessVersion");
        int replicasBinlogSizeIdx = replicasTitles.indexOf("BinlogSize");
        int replicasBinlogFileNumIdx = replicasTitles.indexOf("BinlogFileNum");

        long tabletId = 10001L;
        long backendId = 10002L;

        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        TabletInvertedIndex tabletInvertedIndex = Mockito.mock(TabletInvertedIndex.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Partition partition = Mockito.mock(Partition.class);
        MaterializedIndex index = Mockito.mock(MaterializedIndex.class);
        Tablet tablet = Mockito.mock(Tablet.class);
        Replica replica = Mockito.mock(Replica.class);

        TabletMeta tabletMeta = new TabletMeta(20001L, 20002L, 20003L, 20004L, 12345, TStorageMedium.HDD);

        Mockito.when(systemInfoService.getAllBackendsByAllCluster()).thenReturn(ImmutableMap.of());
        Mockito.when(tabletInvertedIndex.getTabletMeta(tabletId)).thenReturn(tabletMeta);
        Mockito.when(internalCatalog.getDbNullable(tabletMeta.getDbId())).thenReturn(database);
        Mockito.when(database.getTableNullable(tabletMeta.getTableId())).thenReturn(table);
        Mockito.when(table.getPartition(tabletMeta.getPartitionId())).thenReturn(partition);
        Mockito.when(partition.getIndex(tabletMeta.getIndexId())).thenReturn(index);
        Mockito.when(index.getTablet(tabletId)).thenReturn(tablet);

        Mockito.when(replica.getId()).thenReturn(6006L);
        Mockito.when(replica.getBackendIdWithoutException()).thenReturn(backendId);
        Mockito.when(replica.getVersion()).thenReturn(101L);
        Mockito.when(replica.getLastSuccessVersion()).thenReturn(100L);
        Mockito.when(replica.getBinlogSize()).thenReturn(8192L);
        Mockito.when(replica.getBinlogFileNum()).thenReturn(5L);

        try (MockedStatic<Env> mockedEnvStatic = Mockito.mockStatic(Env.class)) {
            mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            mockedEnvStatic.when(Env::getCurrentInvertedIndex).thenReturn(tabletInvertedIndex);
            mockedEnvStatic.when(Env::getCurrentInternalCatalog).thenReturn(internalCatalog);

            ProcResult result = new ReplicasProcNode(tabletId, Collections.singletonList(replica)).fetchResult();
            List<List<String>> rows = result.getRows();
            Assert.assertEquals(1, rows.size());

            List<String> row = rows.get(0);
            Assert.assertEquals(replicasTitles.size(), row.size());
            Assert.assertEquals("6006", row.get(replicaIdIdx));
            Assert.assertEquals("10002", row.get(backendIdIdx));
            Assert.assertEquals("101", row.get(versionIdx));
            Assert.assertEquals("100", row.get(lastSuccessVersionIdx));
            Assert.assertEquals("8192", row.get(replicasBinlogSizeIdx));
            Assert.assertEquals("5", row.get(replicasBinlogFileNumIdx));
        }
    }

}
