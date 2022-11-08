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

package org.apache.doris.statistics;

import org.apache.doris.catalog.Env;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

public class StatisticsCleanerTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        try {
            StatisticStorageInitializer.createDB();
            createDatabase("test");
            connectContext.setDatabase("default_cluster:test");
            createTable(
                    "CREATE TABLE t_p1 (\n" + "    id BIGINT,\n" + "    val BIGINT\n" + ") DUPLICATE KEY(`id`)\n"
                    + "PARTITION BY RANGE(`id`)\n" + "(\n" + "    PARTITION `p1` VALUES LESS THAN ('5'),\n"
                    + "    PARTITION `p2` VALUES LESS THAN ('10')\n" + ")\n" + "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n"
                    + "PROPERTIES (\n" + "\"replication_num\"=\"1\"\n" + ");");
            createTable(
                    "CREATE TABLE t_p2 (\n" + "    id BIGINT,\n" + "    val BIGINT\n" + ") DUPLICATE KEY(`id`)\n"
                    + "PARTITION BY RANGE(`id`)\n" + "(\n" + "    PARTITION `p1` VALUES LESS THAN ('5'),\n"
                    + "    PARTITION `p2` VALUES LESS THAN ('10')\n" + ")\n" + "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n"
                    + "PROPERTIES (\n" + "\"replication_num\"=\"1\"\n" + ");");
            StatisticStorageInitializer storageInitializer = new StatisticStorageInitializer();
            Env.getCurrentEnv().createTable(storageInitializer.buildAnalysisJobTblStmt());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testClean() {

    }
}
