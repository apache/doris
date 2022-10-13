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
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.resource.Tag;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class ReplicaAllocationTest {

    @Test
    public void testNormal() throws AnalysisException {
        // DEFAULT_ALLOCATION
        ReplicaAllocation replicaAlloc = ReplicaAllocation.DEFAULT_ALLOCATION;
        Assert.assertFalse(replicaAlloc.isNotSet());
        Assert.assertTrue(replicaAlloc.equals(ReplicaAllocation.DEFAULT_ALLOCATION));
        Assert.assertFalse(replicaAlloc.isEmpty());
        Assert.assertEquals(3, replicaAlloc.getTotalReplicaNum());
        Assert.assertEquals("tag.location.default: 3", replicaAlloc.toCreateStmt());

        // NOT SET
        replicaAlloc = ReplicaAllocation.NOT_SET;
        Assert.assertTrue(replicaAlloc.isNotSet());
        Assert.assertFalse(replicaAlloc.equals(ReplicaAllocation.DEFAULT_ALLOCATION));
        Assert.assertTrue(replicaAlloc.isEmpty());
        Assert.assertEquals(0, replicaAlloc.getTotalReplicaNum());
        Assert.assertEquals("", replicaAlloc.toCreateStmt());

        // set replica num
        replicaAlloc = new ReplicaAllocation((short) 5);
        Assert.assertFalse(replicaAlloc.isNotSet());
        Assert.assertFalse(replicaAlloc.equals(ReplicaAllocation.DEFAULT_ALLOCATION));
        Assert.assertFalse(replicaAlloc.isEmpty());
        Assert.assertEquals(5, replicaAlloc.getTotalReplicaNum());
        Assert.assertEquals("tag.location.default: 5", replicaAlloc.toCreateStmt());

        // set replica num with tag
        replicaAlloc = new ReplicaAllocation();
        replicaAlloc.put(Tag.create(Tag.TYPE_LOCATION, "zone1"), (short) 3);
        replicaAlloc.put(Tag.create(Tag.TYPE_LOCATION, "zone2"), (short) 2);
        Assert.assertFalse(replicaAlloc.isNotSet());
        Assert.assertFalse(replicaAlloc.isEmpty());
        Assert.assertEquals(5, replicaAlloc.getTotalReplicaNum());
        Assert.assertEquals("tag.location.zone2: 2, tag.location.zone1: 3", replicaAlloc.toCreateStmt());
    }

    @Test
    public void testPropertyAnalyze() throws AnalysisException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "3");
        ReplicaAllocation replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(properties, "");
        Assert.assertEquals(ReplicaAllocation.DEFAULT_ALLOCATION, replicaAlloc);
        Assert.assertTrue(properties.isEmpty());

        // not set
        properties = Maps.newHashMap();
        replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(properties, "");
        Assert.assertEquals(ReplicaAllocation.NOT_SET, replicaAlloc);

        properties = Maps.newHashMap();
        properties.put("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "3");
        replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(properties, "default");
        Assert.assertEquals(ReplicaAllocation.DEFAULT_ALLOCATION, replicaAlloc);
        Assert.assertTrue(properties.isEmpty());

        properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION, "tag.location.zone2: 2, tag.location.zone1: 3");
        replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(properties, "");
        Assert.assertNotEquals(ReplicaAllocation.DEFAULT_ALLOCATION, replicaAlloc);
        Assert.assertFalse(replicaAlloc.isNotSet());
        Assert.assertFalse(replicaAlloc.isEmpty());
        Assert.assertEquals(5, replicaAlloc.getTotalReplicaNum());
        Assert.assertEquals("tag.location.zone2: 2, tag.location.zone1: 3", replicaAlloc.toCreateStmt());
        Assert.assertTrue(properties.isEmpty());

        properties = Maps.newHashMap();
        properties.put("dynamic_partition." + PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION, "tag.location.zone2: 1, tag.location.zone1: 3");
        replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(properties, "dynamic_partition");
        Assert.assertNotEquals(ReplicaAllocation.DEFAULT_ALLOCATION, replicaAlloc);
        Assert.assertFalse(replicaAlloc.isNotSet());
        Assert.assertFalse(replicaAlloc.isEmpty());
        Assert.assertEquals(4, replicaAlloc.getTotalReplicaNum());
        Assert.assertEquals("tag.location.zone2: 1, tag.location.zone1: 3", replicaAlloc.toCreateStmt());
        Assert.assertTrue(properties.isEmpty());
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
        Assert.assertEquals(replicaAlloc, newAlloc);

        // 3. delete files
        dis.close();
        Files.deleteIfExists(path);
    }
}
