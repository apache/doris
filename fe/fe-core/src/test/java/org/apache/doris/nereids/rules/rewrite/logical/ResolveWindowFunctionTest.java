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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

public class ResolveWindowFunctionTest extends TestWithFeService implements PatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        createTable("CREATE TABLE IF NOT EXISTS `supplier` (\n"
                + "  `s_suppkey` int(11) NOT NULL COMMENT \"\",\n"
                + "  `s_name` varchar(26) NOT NULL COMMENT \"\",\n"
                + "  `s_address` varchar(26) NOT NULL COMMENT \"\",\n"
                + "  `s_city` varchar(11) NOT NULL COMMENT \"\",\n"
                + "  `s_nation` varchar(16) NOT NULL COMMENT \"\",\n"
                + "  `s_region` varchar(13) NOT NULL COMMENT \"\",\n"
                + "  `s_phone` varchar(16) NOT NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`s_suppkey`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"colocate_with\" = \"groupa4\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"DEFAULT\"\n"
                + ")");
    }

    @Test
    public void testAnalyze() {
        // String sql = "SELECT sum(s_suppkey) OVER(PARTITION BY s_nation ORDER BY s_name) FROM supplier";
        // String sql2 = "SELECT s_city, sum(s_suppkey) FROM supplier GROUP BY s_city ORDER BY s_city limit 10";
        // String sql2 = "SELECT s_city FROM supplier GROUP BY s_city ORDER BY s_city limit 10";
        // String sql2 = "SELECT s_city FROM supplier ORDER BY s_city limit 10";
        // String sql2 = "SELECT s_city FROM supplier ORDER BY s_city limit 10";
        // String sql = "select s_address, rank() over(order by s_suppkey), row_number() over(order by s_city) from supplier";
        // String sql2 = "select s_address, rank() over(partition by s_nation), row_number() over(partition by s_city order by s_nation) from supplier";
        // String sql = "select ntile(5) over(partition by s_city) from supplier";
        // String sql = "select sum(s_suppkey) over(partition by count(s_city)) from supplier";
        // String sql = "SELECT s_city, row_number() over(PARTITION BY s_address ORDER BY s_nation) FROM supplier";
        // String sql2 = "select s_city, row_number() over(PARTITION BY s_address ORDER BY s_nation) from supplier";
        // String sql = "select s_suppkey+2, rank() over(partition by s_suppkey+1 order by s_suppkey+3) from supplier";
        // String sql2 = "select s_nation, s_suppkey+3, sum(s_suppkey+2) over(partition by s_suppkey+3 order by s_city) from supplier";
        // String sql = "select sum(s_suppkey+2) over(partition by s_suppkey+3 order by s_city) from supplier group by s_city, s_suppkey";
        // String sql3 = "select rank() over(partition by s_suppkey+3 order by s_city) from supplier group by s_city, s_suppkey";
        // String sql = "select sum(s_suppkey) over(partition by s_city) from supplier";
        String sql = " select rank() over from supplier";
        String sql2 = "select s_suppkey+1, s_city, sum(s_suppkey), sum(s_suppkey+1) over(partition by s_city order by s_suppkey + 1) from supplier group by s_city, s_suppkey";
        PlanChecker.from(connectContext).checkPlannerResult(sql);
}
