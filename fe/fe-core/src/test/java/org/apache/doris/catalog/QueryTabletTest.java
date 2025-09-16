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

import org.apache.doris.system.Backend;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryTabletTest extends TestWithFeService {

    @Override
    protected int backendNum() {
        return 3;
    }

    @Test
    public void testTabletOnBadDisks() throws Exception {
        createDatabase("db1");
        createTable("create table db1.tbl1(k1 int) distributed by hash(k1) buckets 1"
                + " properties('replication_num' = '3')");

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("db1");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("tbl1");
        Assertions.assertNotNull(tbl);
        Tablet tablet = tbl.getPartitions().iterator().next()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL).iterator().next()
                .getTablets().iterator().next();

        List<Replica> replicas = tablet.getReplicas();
        Assertions.assertEquals(3, replicas.size());
        for (Replica replica : replicas) {
            Assertions.assertTrue(replica.getPathHash() != -1L);
        }

        Assertions.assertEquals(replicas,
                tablet.getQueryableReplicas(1L, getAlivePathHashs(), false));

        // disk mark as bad
        Env.getCurrentSystemInfo().getBackend(replicas.get(0).getBackendId())
                .getDisks().values().forEach(disk -> disk.setState(DiskInfo.DiskState.OFFLINE));

        // lost disk
        replicas.get(1).setPathHash(-123321L);

        Assertions.assertEquals(Lists.newArrayList(replicas.get(2)),
                tablet.getQueryableReplicas(1L, getAlivePathHashs(), false));
    }

    private Map<Long, Set<Long>> getAlivePathHashs() {
        Map<Long, Set<Long>> backendAlivePathHashs = Maps.newHashMap();
        for (Backend backend : Env.getCurrentSystemInfo().getAllClusterBackendsNoException().values()) {
            backendAlivePathHashs.put(backend.getId(), backend.getDisks().values().stream()
                    .filter(DiskInfo::isAlive).map(DiskInfo::getPathHash).collect(Collectors.toSet()));
        }

        return backendAlivePathHashs;
    }

}

