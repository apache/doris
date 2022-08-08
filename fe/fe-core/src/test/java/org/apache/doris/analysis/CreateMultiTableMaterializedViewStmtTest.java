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

package org.apache.doris.analysis;

import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

public class CreateMultiTableMaterializedViewStmtTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");
    }

    @Test
    public void testSimple() throws Exception {
        createTable("create table test.t1 (pk int, v1 int sum) aggregate key (pk) "
                + "distributed by hash (pk) buckets 1 properties ('replication_num' = '1');");
        createTable("create table test.t2 (pk int, v2 int sum) aggregate key (pk) "
                + "distributed by hash (pk) buckets 1 properties ('replication_num' = '1');");
        StmtExecutor executor = new StmtExecutor(connectContext, "create materialized view mv "
                + "build immediate refresh complete distributed by hash (mv_pk) "
                + "as select test.t1.pk as mv_pk from test.t1, test.t2 where test.t1.pk = test.t2.pk");
        executor.execute();
    }
}
