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

import org.apache.doris.common.DdlException;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;

import java.util.List;

public class CreateTableElasticOnStorageMediumTest extends TestWithFeService {

    @Override
    protected void runAfterAll() throws Exception {
        Env.getCurrentEnv().clear();
    }

    public void setStorageMediumToSSDTest() throws Exception {
        SystemInfoService clusterInfo = Env.getCurrentEnv().getClusterInfo();
        List<Backend> allBackends = clusterInfo.getAllBackendsByAllCluster().values().asList();
        // set all backends' storage medium to SSD
        for (Backend backend : allBackends) {
            if (backend.hasPathHash()) {
                backend.getDisks().values().stream()
                        .peek(diskInfo -> diskInfo.setStorageMedium(TStorageMedium.SSD));
            }
        }
        createDatabase("db1");

        String sql1 = "CREATE TABLE IF NOT EXISTS db1.t1 (pk INT, v1 INT sum) AGGREGATE KEY (pk) "
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 PROPERTIES ('replication_num' = '1');";
        Assertions.assertDoesNotThrow(() -> createTables(sql1));
        String sql2 = "CREATE TABLE IF NOT EXISTS db1.t2 (pk INT, v1 INT sum) AGGREGATE KEY (pk) "
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 PROPERTIES ('replication_num' = '1', 'storage_medium' = 'ssd');";
        Assertions.assertDoesNotThrow(() -> createTables(sql2));
        String sql3 = "CREATE TABLE IF NOT EXISTS db1.t3 (pk INT, v1 INT sum) AGGREGATE KEY (pk) "
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 PROPERTIES ('replication_num' = '1', 'storage_medium' = 'hdd');";
        Assertions.assertThrows(DdlException.class, () -> createTables(sql3));
    }

    public void setStorageMediumToHDDTest() throws Exception {
        SystemInfoService clusterInfo = Env.getCurrentEnv().getClusterInfo();
        List<Backend> allBackends = clusterInfo.getAllBackendsByAllCluster().values().asList();
        // set all backends' storage medium to SSD
        for (Backend backend : allBackends) {
            if (backend.hasPathHash()) {
                backend.getDisks().values().stream()
                        .peek(diskInfo -> diskInfo.setStorageMedium(TStorageMedium.HDD));
            }
        }
        createDatabase("db1");

        String sql1 = "CREATE TABLE IF NOT EXISTS db1.t4 (pk INT, v1 INT sum) AGGREGATE KEY (pk) "
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 PROPERTIES ('replication_num' = '1');";
        Assertions.assertDoesNotThrow(() -> createTables(sql1));
        String sql2 = "CREATE TABLE IF NOT EXISTS db1.t5 (pk INT, v1 INT sum) AGGREGATE KEY (pk) "
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 PROPERTIES ('replication_num' = '1', 'storage_medium' = 'hdd');";
        Assertions.assertDoesNotThrow(() -> createTables(sql2));
        String sql3 = "CREATE TABLE IF NOT EXISTS db1.t6 (pk INT, v1 INT sum) AGGREGATE KEY (pk) "
                + "DISTRIBUTED BY HASH(pk) BUCKETS 1 PROPERTIES ('replication_num' = '1', 'storage_medium' = 'ssd');";
        Assertions.assertThrows(DdlException.class, () -> createTables(sql3));
    }

}
