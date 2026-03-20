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
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
    public void testCloudColocationGroupDetailWithoutTag() throws Exception {
        createTable("CREATE TABLE colocate_t1 (k INT) DISTRIBUTED BY HASH(k) BUCKETS 2 "
                + "PROPERTIES ('replication_num' = '1', 'colocate_with' = 'g1')");
        createTable("CREATE TABLE colocate_t2 (k INT) DISTRIBUTED BY HASH(k) BUCKETS 2 "
                + "PROPERTIES ('replication_num' = '1', 'colocate_with' = 'g1')");

        OlapTable table1 = (OlapTable) db.getTableOrMetaException("colocate_t1");
        GroupId groupId = Env.getCurrentColocateIndex().getGroup(table1.getId());
        Assertions.assertNotNull(groupId);

        new MockUp<Config>() {
            @Mock
            public boolean isCloudMode() {
                return true;
            }
        };
        new MockUp<ColocateTableIndex>() {
            @Mock
            public Map<Tag, List<List<Long>>> getBackendsPerBucketSeq(GroupId gid) {
                return Maps.newHashMap();
            }
        };
        ProcNodeInterface node = new ColocationGroupProcDir().lookup(groupId.toString());
        ProcResult result = node.fetchResult();
        Assertions.assertEquals(Lists.newArrayList("BucketIndex", "BackendIds"), result.getColumnNames());
        Assertions.assertFalse(result.getRows().isEmpty());
        Assertions.assertTrue(result.getRows().stream().anyMatch(row -> row.size() == 2 && !row.get(1).isEmpty()));
    }

    @Test
    public void testCloudColocationGroupReplicaAllocationIsNull() throws Exception {
        createTable("CREATE TABLE colocate_t3 (k INT) DISTRIBUTED BY HASH(k) BUCKETS 2 "
                + "PROPERTIES ('replication_num' = '1', 'colocate_with' = 'g2')");
        createTable("CREATE TABLE colocate_t4 (k INT) DISTRIBUTED BY HASH(k) BUCKETS 2 "
                + "PROPERTIES ('replication_num' = '1', 'colocate_with' = 'g2')");

        new MockUp<Config>() {
            @Mock
            public boolean isCloudMode() {
                return true;
            }
        };

        ProcResult result = new ColocationGroupProcDir().fetchResult();
        int groupNameIdx = ColocationGroupProcDir.TITLE_NAMES.indexOf("GroupName");
        int replicaAllocIdx = ColocationGroupProcDir.TITLE_NAMES.indexOf("ReplicaAllocation");
        List<String> groupRow = result.getRows().stream()
                .filter(row -> "test.g2".equals(row.get(groupNameIdx)))
                .findFirst()
                .orElseThrow(() -> new AssertionError("can not find colocate group test.g2"));
        Assertions.assertEquals("null", groupRow.get(replicaAllocIdx));
    }
}
