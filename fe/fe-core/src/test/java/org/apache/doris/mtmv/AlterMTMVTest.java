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
import org.apache.doris.common.DdlException;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;


public class AlterMTMVTest extends TestWithFeService {

    @Test
    public void testAlterMTMV() throws Exception {
        createDatabaseAndUse("test");

        createTable("CREATE TABLE `stu` (`sid` int(32) NULL, `sname` varchar(32) NULL)\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`sid`)\n"
                + "DISTRIBUTED BY HASH(`sid`) BUCKETS 1\n"
                + "PROPERTIES ('replication_allocation' = 'tag.location.default: 1')");

        createMvByNereids("CREATE MATERIALIZED VIEW mv_a BUILD IMMEDIATE REFRESH COMPLETE ON COMMIT\n"
                + "DISTRIBUTED BY HASH(`sid`) BUCKETS 1\n"
                + "PROPERTIES ('replication_allocation' = 'tag.location.default: 1') "
                + "AS select * from stu limit 1");

        alterMv("ALTER MATERIALIZED VIEW mv_a RENAME mv_b");

        MTMVRelationManager relationManager = Env.getCurrentEnv().getMtmvService().getRelationManager();
        Table table = Env.getCurrentInternalCatalog().getDb("test").get().getTableOrMetaException("stu");
        Set<MTMV> allMTMVs = relationManager.getCandidateMTMVs(Lists.newArrayList(new BaseTableInfo(table)));
        boolean hasMvA = false;
        boolean hasMvB = false;
        for (MTMV mtmv : allMTMVs) {
            if ("mv_a".equals(mtmv.getName())) {
                hasMvA = true;
            }
            if ("mv_b".equals(mtmv.getName())) {
                hasMvB = true;
            }
        }
        Assertions.assertFalse(hasMvA);
        Assertions.assertTrue(hasMvB);


        createTable("CREATE TABLE `stu1` (`sid` int(32) NULL, `sname` varchar(32) NULL)\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`sid`)\n"
                + "DISTRIBUTED BY HASH(`sid`) BUCKETS 1\n"
                + "PROPERTIES ('replication_allocation' = 'tag.location.default: 1')");

        DdlException exception = Assertions.assertThrows(DdlException.class, () ->
                alterTableSync("ALTER TABLE stu1 REPLACE WITH TABLE mv_b PROPERTIES('swap' = 'true')"));
        Assertions.assertEquals("errCode = 2, detailMessage = replace table[mv_b] cannot be a materialized view", exception.getMessage());
    }
}
