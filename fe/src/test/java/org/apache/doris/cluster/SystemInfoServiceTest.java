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

package org.apache.doris.cluster;

import org.apache.doris.analysis.AccessTestUtil;
import org.apache.doris.analysis.AddBackendClause;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DropBackendClause;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.persist.EditLog;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Lists;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "org.apache.log4j.*", "javax.management.*" })
@PrepareForTest(Catalog.class)
public class SystemInfoServiceTest {

    private EditLog editLog;
    private Catalog catalog;
    private SystemInfoService systemInfoService;
    private TabletInvertedIndex invertedIndex;
    private Database db;

    private Analyzer analyzer;

    private String hostPort;

    private long backendId = 10000L;

    @Before
    public void setUp() throws IOException {
        editLog = EasyMock.createMock(EditLog.class);
        editLog.logAddBackend(EasyMock.anyObject(Backend.class));
        EasyMock.expectLastCall().anyTimes();
        editLog.logDropBackend(EasyMock.anyObject(Backend.class));
        EasyMock.expectLastCall().anyTimes();
        editLog.logBackendStateChange(EasyMock.anyObject(Backend.class));
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(editLog);

        db = EasyMock.createMock(Database.class);
        db.readLock();
        EasyMock.expectLastCall().anyTimes();
        db.readUnlock();
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(db);

        catalog = EasyMock.createMock(Catalog.class);
        EasyMock.expect(catalog.getNextId()).andReturn(backendId).anyTimes();
        EasyMock.expect(catalog.getEditLog()).andReturn(editLog).anyTimes();
        EasyMock.expect(catalog.getDb(EasyMock.anyLong())).andReturn(db).anyTimes();
        EasyMock.expect(catalog.getCluster(EasyMock.anyString())).andReturn(new Cluster("cluster", 1)).anyTimes();

        catalog.clear();
        EasyMock.expectLastCall().anyTimes();
        EasyMock.replay(catalog);

        PowerMock.mockStatic(Catalog.class);
        systemInfoService = new SystemInfoService();
        invertedIndex = new TabletInvertedIndex();
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        EasyMock.expect(Catalog.getCurrentSystemInfo()).andReturn(systemInfoService).anyTimes();
        EasyMock.expect(Catalog.getCurrentInvertedIndex()).andReturn(invertedIndex).anyTimes();
        EasyMock.expect(Catalog.getCurrentCatalogJournalVersion()).andReturn(FeConstants.meta_version).anyTimes();
        PowerMock.replay(Catalog.class);

        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
    }

    public void mkdir(String dirString) {
        File dir = new File(dirString);
        if (!dir.exists()) {
            dir.mkdir();
        } else {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }
        }
    }

    public void deleteDir(String metaDir) {
        File dir = new File(metaDir);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }

            dir.delete();
        }
    }

    public void createHostAndPort(int type) {
        switch (type) {
            case 1:
                // missing ip
                hostPort = "12346";
                break;
            case 2:
                // invalid ip
                hostPort = "asdasd:12345";
                break;
            case 3:
                // invalid port
                hostPort = "10.1.2.3:123467";
                break;
            case 4:
                // normal
                hostPort = "127.0.0.1:12345";
                break;
            default:
                break;
        }
    }

    public void clearAllBackend() {
        Catalog.getCurrentSystemInfo().dropAllBackend();
    }

    @Test(expected = AnalysisException.class)
    public void validHostAndPortTest1() throws Exception {
        createHostAndPort(1);
        systemInfoService.validateHostAndPort(hostPort);
    }

    @Test(expected = AnalysisException.class)
    public void validHostAndPortTest3() throws Exception {
        createHostAndPort(3);
        systemInfoService.validateHostAndPort(hostPort);
    }

    @Test
    public void validHostAndPortTest4() throws Exception {
        createHostAndPort(4);
        systemInfoService.validateHostAndPort(hostPort);
    }

    @Test
    public void addBackendTest() throws AnalysisException {
        clearAllBackend();
        AddBackendClause stmt = new AddBackendClause(Lists.newArrayList("192.168.0.1:1234"));
        stmt.analyze(analyzer);
        try {
            Catalog.getCurrentSystemInfo().addBackends(stmt.getHostPortPairs(), true);
        } catch (DdlException e) {
            Assert.fail();
        }

        try {
            Catalog.getCurrentSystemInfo().addBackends(stmt.getHostPortPairs(), true);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("already exists"));
        }

        Assert.assertNotNull(Catalog.getCurrentSystemInfo().getBackend(backendId));
        Assert.assertNotNull(Catalog.getCurrentSystemInfo().getBackendWithHeartbeatPort("192.168.0.1", 1234));

        Assert.assertTrue(Catalog.getCurrentSystemInfo().getBackendIds(false).size() == 1);
        Assert.assertTrue(Catalog.getCurrentSystemInfo().getBackendIds(false).get(0) == backendId);

        Assert.assertTrue(Catalog.getCurrentSystemInfo().getBackendReportVersion(backendId) == 0L);

        Catalog.getCurrentSystemInfo().updateBackendReportVersion(backendId, 2L, 20000L);
        Assert.assertTrue(Catalog.getCurrentSystemInfo().getBackendReportVersion(backendId) == 2L);
    }

    @Test
    public void removeBackendTest() throws AnalysisException {
        clearAllBackend();
        AddBackendClause stmt = new AddBackendClause(Lists.newArrayList("192.168.0.1:1234"));
        stmt.analyze(analyzer);
        try {
            Catalog.getCurrentSystemInfo().addBackends(stmt.getHostPortPairs(), true);
        } catch (DdlException e) {
            e.printStackTrace();
        }

        DropBackendClause dropStmt = new DropBackendClause(Lists.newArrayList("192.168.0.1:1234"));
        dropStmt.analyze(analyzer);
        try {
            Catalog.getCurrentSystemInfo().dropBackends(dropStmt.getHostPortPairs());
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            Catalog.getCurrentSystemInfo().dropBackends(dropStmt.getHostPortPairs());
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
    public void testSaveLoadBackend() throws Exception {
        clearAllBackend();
        String dir = "testLoadBackend";
        mkdir(dir);
        File file = new File(dir, "image");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        SystemInfoService systemInfoService = Catalog.getCurrentSystemInfo();
        Backend back1 = new Backend(1L, "localhost", 3);
        back1.updateOnce(4, 6, 8);
        systemInfoService.replayAddBackend(back1);
        long checksum1 = systemInfoService.saveBackends(dos, 0);
        catalog.clear();
        catalog = null;
        dos.close();

        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        long checksum2 = systemInfoService.loadBackends(dis, 0);
        Assert.assertEquals(checksum1, checksum2);
        Assert.assertEquals(1, systemInfoService.getIdToBackend().size());
        Backend back2 = systemInfoService.getBackend(1);
        Assert.assertTrue(back1.equals(back2));
        dis.close();

        deleteDir(dir);
    }

}
