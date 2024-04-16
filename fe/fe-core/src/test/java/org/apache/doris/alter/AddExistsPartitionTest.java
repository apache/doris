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

package org.apache.doris.alter;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.DebugPointUtil.DebugPoint;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class AddExistsPartitionTest extends TestWithFeService {

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        Config.enable_debug_points = true;
    }

    @Test
    public void testAddExistsPartition() throws Exception {
        DebugPointUtil.addDebugPoint("InternalCatalog.addPartition.noCheckExists", new DebugPoint());
        createDatabase("test");
        createTable("CREATE TABLE test.tbl (k INT) DISTRIBUTED BY HASH(k) "
                + " BUCKETS 5 PROPERTIES ( \"replication_num\" = \"" + backendNum() + "\" )");
        List<Long> backendIds = Env.getCurrentSystemInfo().getAllBackendIds();
        for (long backendId : backendIds) {
            Assertions.assertEquals(5, Env.getCurrentInvertedIndex().getTabletIdsByBackendId(backendId).size());
        }

        String addPartitionSql = "ALTER TABLE test.tbl  ADD PARTITION  IF NOT EXISTS tbl"
                + " DISTRIBUTED BY HASH(k) BUCKETS 5";
        Assertions.assertNotNull(getSqlStmtExecutor(addPartitionSql));
        for (long backendId : backendIds) {
            Assertions.assertEquals(5, Env.getCurrentInvertedIndex().getTabletIdsByBackendId(backendId).size());
        }
    }
}
