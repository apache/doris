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

import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.resource.Tag;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

public class ColocationGroupProcDirTest extends TestWithFeService {
    private Database db;

    @Override
    protected int backendNum() {
        return 1;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        for (Table table : db.getTables()) {
            dropTable(table.getName(), true);
        }
    }

    @Test
    public void testLocalColocationGroupDetailKeepsTagColumns() throws Exception {
        Tag tag1 = Tag.create(Tag.TYPE_LOCATION, "tag1");
        Tag tag2 = Tag.create(Tag.TYPE_LOCATION, "tag2");
        Map<Tag, List<List<Long>>> backendsSeq = Maps.newLinkedHashMap();
        backendsSeq.put(tag1, Lists.newArrayList(
                Lists.newArrayList(10001L, 10002L),
                Lists.newArrayList(10003L)));
        backendsSeq.put(tag2, Lists.newArrayList(
                Lists.newArrayList(20001L),
                Lists.newArrayList(20002L, 20003L)));

        ProcResult result = new ColocationGroupBackendSeqsProcNode(backendsSeq).fetchResult();

        Assertions.assertEquals(Lists.newArrayList("BucketIndex", tag1.toString(), tag2.toString()),
                result.getColumnNames());
        Assertions.assertEquals(Lists.newArrayList("0", "10001, 10002", "20001"), result.getRows().get(0));
        Assertions.assertEquals(Lists.newArrayList("1", "10003", "20002, 20003"), result.getRows().get(1));
    }

    @Test
    public void testCloudColocationGroupDetailWithoutTag() throws Exception {
        String originDeployMode = Config.deploy_mode;
        createTable("CREATE TABLE colocate_t1 (k INT) DISTRIBUTED BY HASH(k) BUCKETS 2 "
                + "PROPERTIES ('replication_num' = '1', 'colocate_with' = 'g1')");
        createTable("CREATE TABLE colocate_t2 (k INT) DISTRIBUTED BY HASH(k) BUCKETS 2 "
                + "PROPERTIES ('replication_num' = '1', 'colocate_with' = 'g1')");

        OlapTable table1 = (OlapTable) db.getTableOrMetaException("colocate_t1");
        GroupId groupId = Env.getCurrentColocateIndex().getGroup(table1.getId());
        Assertions.assertNotNull(groupId);

        ColocateTableIndex colocateTableIndex = Mockito.spy(Env.getCurrentColocateIndex());
        Mockito.doReturn(Maps.<Tag, List<List<Long>>>newHashMap()).when(colocateTableIndex)
                .getBackendsPerBucketSeq(groupId);
        Config.deploy_mode = "cloud";
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::getCurrentColocateIndex).thenReturn(colocateTableIndex);
            ProcNodeInterface node = new ColocationGroupProcDir().lookup(groupId.toString());
            ProcResult result = node.fetchResult();
            Assertions.assertEquals(Lists.newArrayList("BucketIndex", "BackendIds"), result.getColumnNames());
            Assertions.assertFalse(result.getRows().isEmpty());
            Assertions.assertTrue(result.getRows().stream().anyMatch(row -> row.size() == 2 && !row.get(1).isEmpty()));
        } finally {
            Config.deploy_mode = originDeployMode;
        }
    }

    @Test
    public void testCloudColocationGroupReplicaAllocationIsNull() throws Exception {
        String originDeployMode = Config.deploy_mode;
        createTable("CREATE TABLE colocate_t3 (k INT) DISTRIBUTED BY HASH(k) BUCKETS 2 "
                + "PROPERTIES ('replication_num' = '1', 'colocate_with' = 'g2')");
        createTable("CREATE TABLE colocate_t4 (k INT) DISTRIBUTED BY HASH(k) BUCKETS 2 "
                + "PROPERTIES ('replication_num' = '1', 'colocate_with' = 'g2')");

        Config.deploy_mode = "cloud";
        try {
            ProcResult result = new ColocationGroupProcDir().fetchResult();
            int groupNameIdx = ColocationGroupProcDir.TITLE_NAMES.indexOf("GroupName");
            int replicaAllocIdx = ColocationGroupProcDir.TITLE_NAMES.indexOf("ReplicaAllocation");
            List<String> groupRow = result.getRows().stream()
                    .filter(row -> "test.g2".equals(row.get(groupNameIdx)))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("can not find colocate group test.g2"));
            Assertions.assertEquals("null", groupRow.get(replicaAllocIdx));
        } finally {
            Config.deploy_mode = originDeployMode;
        }
    }
}
