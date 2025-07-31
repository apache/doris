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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Table;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class MTMVConcurrentTest extends TestWithFeService {
    @Test
    public void testAlterMTMV() throws Exception {
        createDatabaseAndUse("mtmv_concurrent_test");

        createTable("CREATE TABLE `stu` (`sid` int(32) NULL, `sname` varchar(32) NULL)\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`sid`)\n"
                + "DISTRIBUTED BY HASH(`sid`) BUCKETS 1\n"
                + "PROPERTIES ('replication_allocation' = 'tag.location.default: 1')");

        createMvByNereids("CREATE MATERIALIZED VIEW mv_a BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(`sid`) BUCKETS 1\n"
                + "PROPERTIES ('replication_allocation' = 'tag.location.default: 1') "
                + "AS select * from stu limit 1");


        MTMVRelationManager relationManager = Env.getCurrentEnv().getMtmvService().getRelationManager();
        Table table = Env.getCurrentInternalCatalog().getDb("mtmv_concurrent_test").get()
                .getTableOrMetaException("stu");
        BaseTableInfo baseTableInfo = new BaseTableInfo(table);
        MTMV mtmv = (MTMV) Env.getCurrentInternalCatalog().getDb("mtmv_concurrent_test").get()
                .getTableOrMetaException("mv_a");
        MTMVRelation relation = mtmv.getRelation();
        Set<BaseTableInfo> mtmvsByBaseTable = relationManager.getMtmvsByBaseTable(baseTableInfo);
        Assertions.assertEquals(1, mtmvsByBaseTable.size());
        MTMVTask mtmvTask = new MTMVTask(mtmv, relation, null);
        mtmvTask.setStatus(TaskStatus.SUCCESS);

        // Create threads for concurrent operations
        Thread dropThread = new Thread(() -> {
            try {
                dropMvByNereids("drop materialized view mv_a");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        Thread refreshThread = new Thread(() -> {
            relationManager.refreshComplete(mtmv, relation, mtmvTask);
        });

        // Start both threads
        dropThread.start();
        refreshThread.start();

        // Wait for both threads to complete
        dropThread.join();
        refreshThread.join();

        mtmvsByBaseTable = relationManager.getMtmvsByBaseTable(baseTableInfo);
        Assertions.assertEquals(0, mtmvsByBaseTable.size());
    }
}
