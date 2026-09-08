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

import org.apache.doris.common.ClientPool;
import org.apache.doris.common.GenericPool;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TDiskTrashInfo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TrashProcDirTest {

    private GenericPool<BackendService.Client> originalBackendPool;
    private GenericPool<BackendService.Client> mockBackendPool;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void setUp() {
        originalBackendPool = ClientPool.backendPool;
        mockBackendPool = Mockito.mock(GenericPool.class);
        ClientPool.backendPool = mockBackendPool;
    }

    @AfterEach
    public void tearDown() {
        ClientPool.backendPool = originalBackendPool;
    }

    @Test
    public void testTitleNames() {
        List<String> titles = TrashProcDir.TITLE_NAMES;
        Assertions.assertEquals(8, titles.size());
        Assertions.assertEquals("BackendId", titles.get(0));
        Assertions.assertEquals("Backend", titles.get(1));
        Assertions.assertEquals("RootPath", titles.get(2));
        Assertions.assertEquals("State", titles.get(3));
        Assertions.assertEquals("TrashUsedCapacity", titles.get(4));
        Assertions.assertEquals("TrashFileNum", titles.get(5));
        Assertions.assertEquals("DiskCapacity", titles.get(6));
        Assertions.assertEquals("AvailableCapacity", titles.get(7));
    }

    @Test
    public void testTrashProcNodeTitleNames() {
        List<String> titles = TrashProcNode.TITLE_NAMES;
        Assertions.assertEquals(6, titles.size());
        Assertions.assertEquals("RootPath", titles.get(0));
        Assertions.assertEquals("State", titles.get(1));
        Assertions.assertEquals("TrashUsedCapacity", titles.get(2));
        Assertions.assertEquals("TrashFileNum", titles.get(3));
        Assertions.assertEquals("DiskCapacity", titles.get(4));
        Assertions.assertEquals("AvailableCapacity", titles.get(5));
    }

    @Test
    public void testGetTrashInfoWithEmptyBackends() {
        List<List<String>> infos = new ArrayList<>();
        TrashProcDir.getTrashInfo(new ArrayList<>(), infos);
        Assertions.assertTrue(infos.isEmpty());
    }

    @Test
    public void testGetTrashInfoWithRpcFailure() throws Exception {
        Backend backend = new Backend(10001L, "host1", 9050);
        backend.setBePort(9060);

        Mockito.when(mockBackendPool.borrowObject(Mockito.any()))
                .thenThrow(new RuntimeException("connection refused"));

        List<List<String>> infos = new ArrayList<>();
        TrashProcDir.getTrashInfo(Arrays.asList(backend), infos);

        // Should have 1 fallback row
        Assertions.assertEquals(1, infos.size(), "Expected 1 fallback row, but got: " + infos.size());

        // Verify fallback row content
        List<String> row = infos.get(0);
        Assertions.assertEquals(8, row.size(), "Fallback row should have 8 columns");
        Assertions.assertEquals("10001", row.get(0), "BackendId mismatch");
        Assertions.assertEquals("N/A", row.get(2), "RootPath should be N/A");
        Assertions.assertEquals("UNKNOWN", row.get(3), "State should be UNKNOWN");
        Assertions.assertEquals("N/A", row.get(4), "TrashUsedCapacity should be N/A");
        Assertions.assertEquals("", row.get(5), "TrashFileNum should be empty string");
        Assertions.assertEquals("N/A", row.get(6), "DiskCapacity should be N/A");
        Assertions.assertEquals("N/A", row.get(7), "AvailableCapacity should be N/A");
    }

    @Test
    public void testGetTrashInfoWithAllOptionalFields() throws Exception {
        Backend backend = new Backend(10001L, "host1", 9050);
        backend.setBePort(9060);

        BackendService.Client client = Mockito.mock(BackendService.Client.class);
        Mockito.when(mockBackendPool.borrowObject(Mockito.any())).thenReturn(client);

        TDiskTrashInfo diskInfo = new TDiskTrashInfo("/data1", "ONLINE", 2L * 1024 * 1024 * 1024);
        diskInfo.setTrashFileNum(5L);
        diskInfo.setDiskCapacity(2L * 1024 * 1024 * 1024 * 1024);
        diskInfo.setAvailableCapacity(2L * 1024 * 1024 * 1024 * 1024);
        Mockito.when(client.getDiskTrashUsedCapacity()).thenReturn(Arrays.asList(diskInfo));

        List<List<String>> infos = new ArrayList<>();
        TrashProcDir.getTrashInfo(Arrays.asList(backend), infos);

        Assertions.assertEquals(1, infos.size());
        List<String> row = infos.get(0);
        Assertions.assertEquals(8, row.size());
        Assertions.assertEquals("10001", row.get(0));
        Assertions.assertEquals("/data1", row.get(2));
        Assertions.assertEquals("ONLINE", row.get(3));
        Assertions.assertTrue(row.get(4).contains("GB"), "TrashUsedCapacity should contain GB, got: " + row.get(4));
        Assertions.assertEquals("5", row.get(5));
        Assertions.assertTrue(row.get(6).contains("TB"), "DiskCapacity should contain TB, got: " + row.get(6));
        Assertions.assertTrue(row.get(7).contains("TB"), "AvailableCapacity should contain TB, got: " + row.get(7));
    }

    @Test
    public void testGetTrashInfoWithoutOptionalFields() throws Exception {
        Backend backend = new Backend(10002L, "host2", 9050);
        backend.setBePort(9060);

        BackendService.Client client = Mockito.mock(BackendService.Client.class);
        Mockito.when(mockBackendPool.borrowObject(Mockito.any())).thenReturn(client);

        TDiskTrashInfo diskInfo = new TDiskTrashInfo("/data2", "OFFLINE", 0L);
        Mockito.when(client.getDiskTrashUsedCapacity()).thenReturn(Arrays.asList(diskInfo));

        List<List<String>> infos = new ArrayList<>();
        TrashProcDir.getTrashInfo(Arrays.asList(backend), infos);

        Assertions.assertEquals(1, infos.size());
        List<String> row = infos.get(0);
        Assertions.assertEquals(8, row.size());
        Assertions.assertEquals("10002", row.get(0));
        Assertions.assertEquals("/data2", row.get(2));
        Assertions.assertEquals("OFFLINE", row.get(3));
        Assertions.assertEquals("", row.get(5));
        Assertions.assertEquals("", row.get(6));
        Assertions.assertEquals("", row.get(7));
    }

    @Test
    public void testGetTrashInfoWithMultipleDisks() throws Exception {
        Backend backend = new Backend(10003L, "host3", 9050);
        backend.setBePort(9060);

        BackendService.Client client = Mockito.mock(BackendService.Client.class);
        Mockito.when(mockBackendPool.borrowObject(Mockito.any())).thenReturn(client);

        TDiskTrashInfo disk1 = new TDiskTrashInfo("/data1", "ONLINE", 2L * 1024 * 1024 * 1024);
        disk1.setTrashFileNum(3L);
        disk1.setDiskCapacity(2L * 1024 * 1024 * 1024 * 1024);
        disk1.setAvailableCapacity(1024L * 1024 * 1024 * 1024);

        TDiskTrashInfo disk2 = new TDiskTrashInfo("/data2", "ONLINE", 2L * 1024 * 1024);
        disk2.setTrashFileNum(1L);
        disk2.setDiskCapacity(2L * 1024 * 1024 * 1024);
        disk2.setAvailableCapacity(1024L * 1024 * 1024);

        Mockito.when(client.getDiskTrashUsedCapacity()).thenReturn(Arrays.asList(disk1, disk2));

        List<List<String>> infos = new ArrayList<>();
        TrashProcDir.getTrashInfo(Arrays.asList(backend), infos);

        Assertions.assertEquals(2, infos.size());
        Assertions.assertEquals("/data1", infos.get(0).get(2));
        Assertions.assertEquals("3", infos.get(0).get(5));
        Assertions.assertEquals("/data2", infos.get(1).get(2));
        Assertions.assertEquals("1", infos.get(1).get(5));
    }

    @Test
    public void testGetTrashInfoWithPartialOptionalFields() throws Exception {
        Backend backend = new Backend(10004L, "host4", 9050);
        backend.setBePort(9060);

        BackendService.Client client = Mockito.mock(BackendService.Client.class);
        Mockito.when(mockBackendPool.borrowObject(Mockito.any())).thenReturn(client);

        TDiskTrashInfo diskInfo = new TDiskTrashInfo("/data1", "ONLINE", 1073741824L);
        diskInfo.setTrashFileNum(2L);
        Mockito.when(client.getDiskTrashUsedCapacity()).thenReturn(Arrays.asList(diskInfo));

        List<List<String>> infos = new ArrayList<>();
        TrashProcDir.getTrashInfo(Arrays.asList(backend), infos);

        Assertions.assertEquals(1, infos.size());
        List<String> row = infos.get(0);
        Assertions.assertEquals("2", row.get(5));
        Assertions.assertEquals("", row.get(6));
        Assertions.assertEquals("", row.get(7));
    }

}
