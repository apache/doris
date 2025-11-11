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

package org.apache.doris.load.loadv2;

import org.apache.doris.nereids.trees.plans.commands.insert.AbstractInsertExecutor;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class InsertTest extends TestWithFeService {
    @Test
    public void testParallelOfInsertAutoPartition() throws Exception {
        createDatabase("test");
        useDatabase("test");

        createTable("create table test.tbl(id int, name varchar(255)) auto partition by list(name)()properties('replication_num'='1')");

        AbstractInsertExecutor insertExecutor = PlanChecker.from(connectContext)
                .getInsertExecutor(
                        "insert into test.tbl select * from (select 1, 'test' union all select 2, 'doris')a");
        Assertions.assertEquals(1, insertExecutor.getCoordinator().getFragments().size());
    }
}
