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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.LocalTabletInvertedIndex;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.CountingDataOutputStream;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.trees.plans.commands.info.AddBackendOp;
import org.apache.doris.nereids.trees.plans.commands.info.DropBackendOp;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class SystemInfoServiceTest {

    private EditLog editLog = Mockito.mock(EditLog.class);
    private Env env = Mockito.mock(Env.class);
    private InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
    private SystemInfoService systemInfoService;
    private TabletInvertedIndex invertedIndex;
    private Database db = Mockito.mock(Database.class);
    private Table table = Mockito.mock(Table.class);
    private MockedStatic<Env> mockedEnvStatic;


    private String hostPort;

    private long backendId = 10000L;

    @BeforeEach
    public void setUp() throws IOException {
        mockedEnvStatic = Mockito.mockStatic(Env.class);

        Mockito.when(env.getNextId()).thenReturn(backendId);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDbNullable(Mockito.anyLong())).thenReturn(db);
        Mockito.when(db.getTableNullable(Mockito.anyLong())).thenReturn(table);

        systemInfoService = new SystemInfoService();
        invertedIndex = new LocalTabletInvertedIndex();

        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnvStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
        mockedEnvStatic.when(Env::getCurrentInvertedIndex).thenReturn(invertedIndex);
        mockedEnvStatic.when(Env::getCurrentEnvJournalVersion).thenReturn(FeConstants.meta_version);
    }

    @AfterEach
    public void tearDown() {
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
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
        Env.getCurrentSystemInfo().dropAllBackend();
    }

    @Test
    public void validHostAndPortTest1() throws Exception {
        Assertions.assertThrows(AnalysisException.class, () -> {
            createHostAndPort(1);
            systemInfoService.validateHostAndPort(hostPort);
        });
    }

    @Test
    public void validHostAndPortTest3() throws Exception {
        Assertions.assertThrows(AnalysisException.class, () -> {
            createHostAndPort(3);
            systemInfoService.validateHostAndPort(hostPort);
        });
    }

    @Test
    public void validHostAndPortTest4() throws Exception {
        createHostAndPort(4);
        systemInfoService.validateHostAndPort(hostPort);
    }

    @Test
    public void addBackendTest() throws UserException {
        clearAllBackend();
        AddBackendOp op = new AddBackendOp(Lists.newArrayList("192.168.0.1:1234"), Maps.newHashMap());
        op.validate(new ConnectContext());
        try {
            Env.getCurrentSystemInfo().addBackends(op.getHostInfos(), true);
        } catch (DdlException e) {
            Assertions.fail();
        }

        try {
            Env.getCurrentSystemInfo().addBackends(op.getHostInfos(), true);
        } catch (DdlException e) {
            Assertions.assertTrue(e.getMessage().contains("already exists"));
        }

        Assertions.assertNotNull(Env.getCurrentSystemInfo().getBackend(backendId));
        Assertions.assertNotNull(Env.getCurrentSystemInfo().getBackendWithHeartbeatPort("192.168.0.1", 1234));

        Assertions.assertTrue(Env.getCurrentSystemInfo().getAllBackendIds(false).size() == 1);
        Assertions.assertTrue(Env.getCurrentSystemInfo().getAllBackendIds(false).get(0) == backendId);

        Assertions.assertTrue(Env.getCurrentSystemInfo().getBackendReportVersion(backendId) == 0L);

        Env.getCurrentSystemInfo().updateBackendReportVersion(backendId, 2L, 20000L, 30000L, true);
        Assertions.assertTrue(Env.getCurrentSystemInfo().getBackendReportVersion(backendId) == 2L);
    }

    @Test
    public void removeBackendTest() throws UserException {
        clearAllBackend();
        AddBackendOp op = new AddBackendOp(Lists.newArrayList("192.168.0.1:1234"), Maps.newHashMap());
        op.validate(new ConnectContext());
        try {
            Env.getCurrentSystemInfo().addBackends(op.getHostInfos(), true);
        } catch (DdlException e) {
            e.printStackTrace();
        }

        DropBackendOp dropBackendOp = new DropBackendOp(Lists.newArrayList("192.168.0.1:1234"), true);
        dropBackendOp.validate(new ConnectContext());
        try {
            Env.getCurrentSystemInfo().dropBackends(dropBackendOp.getHostInfos());
        } catch (DdlException e) {
            e.printStackTrace();
            Assertions.fail();
        }

        try {
            Env.getCurrentSystemInfo().dropBackends(dropBackendOp.getHostInfos());
        } catch (DdlException e) {
            Assertions.assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
    public void removeBackendTestByBackendId() throws UserException {
        clearAllBackend();
        AddBackendOp op = new AddBackendOp(Lists.newArrayList("192.168.0.1:1234"), Maps.newHashMap());
        op.validate(new ConnectContext());
        try {
            Env.getCurrentSystemInfo().addBackends(op.getHostInfos(), true);
        } catch (DdlException e) {
            e.printStackTrace();
        }

        DropBackendOp dropBackendOp = new DropBackendOp(Lists.newArrayList(String.valueOf(backendId)), true);
        dropBackendOp.validate(new ConnectContext());
        try {
            Env.getCurrentSystemInfo().dropBackends(dropBackendOp.getHostInfos());
        } catch (DdlException e) {
            e.printStackTrace();
            Assertions.fail();
        }

        try {
            Env.getCurrentSystemInfo().dropBackends(dropBackendOp.getHostInfos());
        } catch (DdlException e) {
            Assertions.assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
    public void testSaveLoadBackend() throws Exception {
        clearAllBackend();
        String dir = "testLoadBackend";
        mkdir(dir);
        File file = new File(dir, "image");
        file.createNewFile();
        CountingDataOutputStream dos = new CountingDataOutputStream(new FileOutputStream(file));
        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        Backend back1 = new Backend(1L, "localhost", 3);
        back1.updateOnce(4, 6, 8);
        systemInfoService.replayAddBackend(back1);
        long checksum1 = systemInfoService.saveBackends(dos, 0);
        env.clear();
        env = null;
        dos.close();

        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        long checksum2 = systemInfoService.loadBackends(dis, 0);
        Assertions.assertEquals(checksum1, checksum2);
        Assertions.assertEquals(1, systemInfoService.getAllBackendsByAllCluster().size());
        Backend back2 = systemInfoService.getBackend(1);
        Assertions.assertEquals(back1, back2);
        dis.close();

        deleteDir(dir);
    }

}
