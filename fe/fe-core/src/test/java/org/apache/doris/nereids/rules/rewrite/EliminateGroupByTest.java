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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

class EliminateGroupByTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("eliminate_group_by");
        createTable(
                "create table eliminate_group_by.t (\n"
                        + "id int not null,\n"
                        + "name varchar(128),\n"
                        + "age int, sex int"
                        + ")\n"
                        + "UNIQUE KEY(id)\n"
                        + "distributed by hash(id) buckets 10\n"
                        + "properties('replication_num' = '1');"
        );
        connectContext.setDatabase("default_cluster:eliminate_group_by");
    }

    @Test
    void eliminateMax() {
        //  -> select id, age as max(age) from t;
        String sql = "select id, max(age) from t group by id";

        PlanChecker cheker = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite();
        cheker.matches(
                        logicalEmptyRelation().when(p -> p.getProjects().get(0).toSql().equals("id")
                                && p.getProjects().get(1).toSql().equals("age AS `max(age)`")));
    }

    @Test
    void eliminateMin() {
        //  -> select id, age as min(age) from t;
        String sql = "select id, min(age) from t group by id";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalEmptyRelation().when(p -> p.getProjects().get(0).toSql().equals("id")
                                && p.getProjects().get(1).toSql().equals("age AS `min(age)`"))
                );
    }

    @Test
    void eliminateSum() {
        //  -> select id, age as sum(age) from t;
        String sql = "select id, sum(age) from t group by id";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalEmptyRelation().when(p -> p.getProjects().get(0).toSql().equals("id")
                                && p.getProjects().get(1).toSql().equals("cast(age as BIGINT) AS `sum(age)`"))
                );
    }

    @Test
    void eliminateCount() {
        //  -> select id, case when age is not null then 1 else 0 end from t;
        String sql = "select id, count(age) from t group by id";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalEmptyRelation().when(p -> p.getProjects().get(0).toSql().equals("id")
                                && p.getProjects().get(1).toSql()
                                .equals("if(age IS NULL, 0, 1) AS `if(age IS NULL, 0, 1)`")
                        )
                );
    }
}
