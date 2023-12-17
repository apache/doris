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

package org.apache.doris.nereids;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UnsupportedTypeTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        createTables(
                "create table type_tb (\n"
                + "            id int NOT NULL, \n"
                + "            kjsonb jsonb,\n"
                + "            kdcml decimalv3(15, 2),\n"
                + "            karr array<int>,\n"
                + "            `date` bigint(20) NOT NULL\n"
                + "        )\n"
                + "        DUPLICATE KEY(id) \n"
                + "        distributed by hash(id) buckets 2\n"
                + "        properties (\n"
                + "            \"replication_num\"=\"1\"\n"
                + "        )",
                "create table type_tb1 (\n"
                        + "            id int NOT NULL, \n"
                        + "            kmap map<string, string>,\n"
                        + "            kstruct struct<a: int, b: int>\n"
                        + "        )\n"
                        + "        DUPLICATE KEY(id) \n"
                        + "        distributed by hash(id) buckets 2\n"
                        + "        properties (\n"
                        + "            \"replication_num\"=\"1\"\n"
                        + "        )");
    }

    @Test
    public void testUnsupportedTypeThrowException() {
        String[] sqls = {
                "select id from type_tb",
                "select karr from type_tb",
                "select array_range(10)",
                "select kmap from type_tb1",
                "select * from type_tb1",
                "select * from type_tb",
        };
        for (int i = 0; i < sqls.length; ++i) {
            runPlanner(sqls[i]);
        }
    }

    @Test
    public void testGroupByAndHavingUseAliasFirstThrowException() {
        String[] sqls = {"SELECT\n"
                + "            date_format(date, '%x%v') AS `date`,\n"
                + "            count(date) AS `diff_days`\n"
                + "            FROM type_tb\n"
                + "            GROUP BY date\n"
                + "            HAVING date = 20221111\n"
                + "            ORDER BY date;",
                "SELECT\n"
                        + "            date_format(date, '%x%v') AS `date`,\n"
                        + "            count(date) AS `diff_days`\n"
                        + "            FROM type_tb\n"
                        + "            GROUP BY date\n"
                        + "            HAVING date = 20221111\n"
                        + "            ORDER BY date;"
        };
        runPlanner(sqls[0]);
        connectContext.getSessionVariable().groupByAndHavingUseAliasFirst = true;
        Assertions.assertThrows(AnalysisException.class, () -> runPlanner(sqls[1]));
    }

    private void runPlanner(String sql) {
        new NereidsPlanner(MemoTestUtils.createStatementContext(connectContext, sql)).plan(
                new NereidsParser().parseSingle(sql),
                PhysicalProperties.ANY
        );
    }
}
