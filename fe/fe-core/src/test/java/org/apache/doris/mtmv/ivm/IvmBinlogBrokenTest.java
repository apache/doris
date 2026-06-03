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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IvmBinlogBrokenTest extends TestWithFeService {

    @Test
    public void testTruncateMarksBinlogBroken() throws Exception {
        String db = "ivm_broken_truncate";
        createPartitionedIvmTableAndMv(db);

        executeSql("TRUNCATE TABLE ivm_base");

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testTruncatePartitionMarksBinlogBroken() throws Exception {
        String db = "ivm_broken_truncate_partition";
        createPartitionedIvmTableAndMv(db);

        executeSql("TRUNCATE TABLE ivm_base PARTITION(p202001)");

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testDropPartitionMarksBinlogBroken() throws Exception {
        String db = "ivm_broken_drop_partition";
        createPartitionedIvmTableAndMv(db);

        executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testReplacePartitionMarksBinlogBroken() throws Exception {
        String db = "ivm_broken_replace_partition";
        createPartitionedIvmTableAndMv(db);

        executeSql("ALTER TABLE ivm_base ADD TEMPORARY PARTITION tp202001 "
                + "VALUES [('2020-01-01'), ('2020-02-01'))");
        executeSql("ALTER TABLE ivm_base REPLACE PARTITION (p202001) "
                + "WITH TEMPORARY PARTITION (tp202001)");

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testRecoverPartitionMarksBinlogBroken() throws Exception {
        String db = "ivm_broken_recover_partition";
        createPartitionedIvmTableAndMv(db);
        executeSql("ALTER TABLE ivm_base DROP PARTITION p202001");
        setBinlogBroken(getMtmv(db), false);

        executeSql("RECOVER PARTITION p202001 FROM ivm_base");

        Assertions.assertTrue(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testAddPartitionDoesNotMarkBinlogBroken() throws Exception {
        String db = "ivm_broken_add_partition";
        createPartitionedIvmTableAndMv(db);

        executeSql("ALTER TABLE ivm_base ADD PARTITION p202003 "
                + "VALUES [('2020-03-01'), ('2020-04-01'))");

        Assertions.assertFalse(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testDropTempPartitionDoesNotMarkBinlogBroken() throws Exception {
        String db = "ivm_broken_drop_temp_partition";
        createPartitionedIvmTableAndMv(db);
        executeSql("ALTER TABLE ivm_base ADD TEMPORARY PARTITION tp202001 "
                + "VALUES [('2020-01-01'), ('2020-02-01'))");

        executeSql("ALTER TABLE ivm_base DROP TEMPORARY PARTITION tp202001");

        Assertions.assertFalse(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    @Test
    public void testDropMissingPartitionIfExistsDoesNotMarkBinlogBroken() throws Exception {
        String db = "ivm_broken_drop_missing_partition";
        createPartitionedIvmTableAndMv(db);

        executeSql("ALTER TABLE ivm_base DROP PARTITION IF EXISTS p_missing");

        Assertions.assertFalse(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    private void createPartitionedIvmTableAndMv(String db) throws Exception {
        createDatabaseAndUse(db);
        createTable("CREATE TABLE " + db + ".ivm_base (\n"
                + "  dt date NOT NULL,\n"
                + "  k1 int,\n"
                + "  v1 int\n"
                + ")\n"
                + "DUPLICATE KEY(dt, k1)\n"
                + "PARTITION BY RANGE(dt) (\n"
                + "  PARTITION p202001 VALUES [('2020-01-01'), ('2020-02-01')),\n"
                + "  PARTITION p202002 VALUES [('2020-02-01'), ('2020-03-01'))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW')");
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_mv\n"
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k1, v1 FROM ivm_base");
        Assertions.assertTrue(getMtmv(db).isIvm());
        Assertions.assertFalse(getMtmv(db).getIvmInfo().isBinlogBroken());
    }

    private MTMV getMtmv(String db) throws Exception {
        return (MTMV) Env.getCurrentInternalCatalog()
                .getDb(db).get()
                .getTableOrMetaException("ivm_mv");
    }

    private void setBinlogBroken(MTMV mtmv, boolean binlogBroken) {
        IvmInfo info = new IvmInfo(mtmv.getIvmInfo());
        info.setBinlogBroken(binlogBroken);
        Env.getCurrentEnv().alterMTMVIvmInfo(
                new TableNameInfo(mtmv.getQualifiedDbName(), mtmv.getName()), info);
    }
}
