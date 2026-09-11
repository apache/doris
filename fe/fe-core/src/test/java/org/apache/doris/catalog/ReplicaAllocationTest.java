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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class ReplicaAllocationTest {
    private SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
    private MockedStatic<Env> mockedEnvStatic;

    @BeforeEach
    public void setUp() throws DdlException {
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

        Mockito.doAnswer(invocation -> Pair.of(Maps.newHashMap(), TStorageMedium.HDD))
                .when(systemInfoService).selectBackendIdsForReplicaCreation(
                        Mockito.any(ReplicaAllocation.class),
                        Mockito.anyMap(),
                        Mockito.nullable(TStorageMedium.class),
                        Mockito.eq(false),
                        Mockito.eq(true));
    }

    @AfterEach
    public void tearDown() {
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
    }

    @Test
    public void testNormal() throws AnalysisException {
        // DEFAULT_ALLOCATION
        ReplicaAllocation replicaAlloc = ReplicaAllocation.DEFAULT_ALLOCATION;
        Assertions.assertFalse(replicaAlloc.isNotSet());
        Assertions.assertEquals(replicaAlloc, ReplicaAllocation.DEFAULT_ALLOCATION);
        Assertions.assertFalse(replicaAlloc.isEmpty());
        Assertions.assertEquals(3, replicaAlloc.getTotalReplicaNum());
        Assertions.assertEquals("tag.location.default: 3", replicaAlloc.toCreateStmt());

        // NOT SET
        replicaAlloc = ReplicaAllocation.NOT_SET;
        Assertions.assertTrue(replicaAlloc.isNotSet());
        Assertions.assertNotEquals(replicaAlloc, ReplicaAllocation.DEFAULT_ALLOCATION);
        Assertions.assertTrue(replicaAlloc.isEmpty());
        Assertions.assertEquals(0, replicaAlloc.getTotalReplicaNum());
        Assertions.assertEquals("", replicaAlloc.toCreateStmt());

        // set replica num
        replicaAlloc = new ReplicaAllocation((short) 5);
        Assertions.assertFalse(replicaAlloc.isNotSet());
        Assertions.assertNotEquals(replicaAlloc, ReplicaAllocation.DEFAULT_ALLOCATION);
        Assertions.assertFalse(replicaAlloc.isEmpty());
        Assertions.assertEquals(5, replicaAlloc.getTotalReplicaNum());
        Assertions.assertEquals("tag.location.default: 5", replicaAlloc.toCreateStmt());

        // set replica num with tag
        replicaAlloc = new ReplicaAllocation();
        replicaAlloc.put(Tag.create(Tag.TYPE_LOCATION, "zone1"), (short) 3);
        replicaAlloc.put(Tag.create(Tag.TYPE_LOCATION, "zone2"), (short) 2);
        Assertions.assertFalse(replicaAlloc.isNotSet());
        Assertions.assertFalse(replicaAlloc.isEmpty());
        Assertions.assertEquals(5, replicaAlloc.getTotalReplicaNum());
        Assertions.assertEquals("tag.location.zone2: 2, tag.location.zone1: 3", replicaAlloc.toCreateStmt());
    }

    @Test
    public void testPropertyAnalyze() throws AnalysisException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "3");
        ReplicaAllocation replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(properties, "");
        Assertions.assertEquals(ReplicaAllocation.DEFAULT_ALLOCATION, replicaAlloc);
        Assertions.assertTrue(properties.isEmpty());

        // not set
        properties = Maps.newHashMap();
        replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(properties, "");
        Assertions.assertEquals(ReplicaAllocation.NOT_SET, replicaAlloc);

        properties = Maps.newHashMap();
        properties.put("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "3");
        replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(properties, "default");
        Assertions.assertEquals(ReplicaAllocation.DEFAULT_ALLOCATION, replicaAlloc);
        Assertions.assertTrue(properties.isEmpty());

        properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION, "tag.location.zone2: 2, tag.location.zone1: 3");
        replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(properties, "");
        Assertions.assertNotEquals(ReplicaAllocation.DEFAULT_ALLOCATION, replicaAlloc);
        Assertions.assertFalse(replicaAlloc.isNotSet());
        Assertions.assertFalse(replicaAlloc.isEmpty());
        Assertions.assertEquals(5, replicaAlloc.getTotalReplicaNum());
        Assertions.assertEquals("tag.location.zone2: 2, tag.location.zone1: 3", replicaAlloc.toCreateStmt());
        Assertions.assertTrue(properties.isEmpty());

        properties = Maps.newHashMap();
        properties.put("dynamic_partition." + PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION, "tag.location.zone2: 1, tag.location.zone1: 3");
        replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(properties, "dynamic_partition");
        Assertions.assertNotEquals(ReplicaAllocation.DEFAULT_ALLOCATION, replicaAlloc);
        Assertions.assertFalse(replicaAlloc.isNotSet());
        Assertions.assertFalse(replicaAlloc.isEmpty());
        Assertions.assertEquals(4, replicaAlloc.getTotalReplicaNum());
        Assertions.assertEquals("tag.location.zone2: 1, tag.location.zone1: 3", replicaAlloc.toCreateStmt());
        Assertions.assertTrue(properties.isEmpty());
    }

    @Test
    public void testAbnormal() {
        final Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION, "3");
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Invalid replication allocation property: 3",
                () -> PropertyAnalyzer.analyzeReplicaAllocation(properties, ""));

        properties.clear();
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION, "tag.location.12321:1");
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Invalid tag value format: 12321",
                () -> PropertyAnalyzer.analyzeReplicaAllocation(properties, ""));
    }

    @Test
    public void testPersist() throws IOException, AnalysisException {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeConstants.meta_version);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        Path path = Files.createFile(Paths.get("./replicaInfo"));
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));

        ReplicaAllocation replicaAlloc = new ReplicaAllocation();
        replicaAlloc.put(Tag.create(Tag.TYPE_LOCATION, "zone1"), (short) 3);
        replicaAlloc.put(Tag.create(Tag.TYPE_LOCATION, "zone2"), (short) 2);
        replicaAlloc.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));
        ReplicaAllocation newAlloc = ReplicaAllocation.read(dis);
        Assertions.assertEquals(replicaAlloc, newAlloc);

        // 3. delete files
        dis.close();
        Files.deleteIfExists(path);
    }
}
