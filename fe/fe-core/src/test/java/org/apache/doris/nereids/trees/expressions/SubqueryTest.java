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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.datasets.tpch.AnalyzeCheckTestBase;

import org.junit.jupiter.api.Test;

public class SubqueryTest extends AnalyzeCheckTestBase {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        String t0 = "create table t0("
                + "id int, \n"
                + "k1 int, \n"
                + "k2 int, \n"
                + "v1 int, \n"
                + "v2 int)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');";

        String t1 = "create table t1("
                + "id int, \n"
                + "k1 int, \n"
                + "v2 int)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');";

        String t2 = "create table t2("
                + "id int, \n"
                + "k1 int, \n"
                + "v2 int)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');";
        createTables(t0, t1, t2);
    }

    @Test
    public void inTest() {
        String sql = "select t0.k1\n"
                + "from t0\n"
                + "where t0.k2 in\n"
                + "    (select id\n"
                + "     from t1\n"
                + "     where t0.k2=t1.k1)";
        checkAnalyze(sql);
    }

    @Test
    public void existTest() {
        String sql1 = "select * from t0 where exists (select * from t1 where t0.k1 = t1.k1);";
        checkAnalyze(sql1);
    }

    @Test
    public void existAndExistTest() {
        String sql1 = "select * from t0 where exists (select * from t1 where t0.k1 = t1.k1) "
                + "and not exists (select * from t2 where t0.id != t2.id);";
        checkAnalyze(sql1);

        // This type of sql cannot be parsed and cannot recognize t1.id.
        // Other systems also cannot resolve such as presto.
        String sql2 = "select * from t0 where exists (select * from t1 where t0.k1 = t1.k1) "
                + "and not exists (select * from t2 where t1.id != t2.id);";
        assert sql2 != null;
    }

    @Test
    public void filterScalarTest() {
        // min will be rewritten as as, it needs to be bound
        /*String sql = "select * from t0 where t0.id = "
                + "(select min(t1.id) from t1 where t0.k1 = t1.k1)";
        checkAnalyze(sql);*/

        // Require that the return value in the where subquery must have only 1 column.
        String sql1 = "select * from t0 where t0.id = "
                + "(select t1.k1, t1.id from t1 where t0.k1 = t1.k1)";
        assert sql1 != null;

        String sql2 = "select * from t0 where t0.id = "
                + "(select sum(t1.k1) from t1 where t0.k1 = t1.k1)";
        checkAnalyze(sql2);

        String sql3 = "select * from t0 where t0.id = "
                + "(select * from t1 where t0.k1 = t1.k1)";
        assert sql3 != null;

        String sql4 = "select * from t0 where t0.id = "
                + "(select sum(t1.k1) from t1 where t0.k1 = t1.k1) "
                + "and t0.k2 = (select sum(t2.k1) from t2 where t0.v1 = t2.v2)";
        checkAnalyze(sql4);

        String sql5 = "select * from t0 where t0.id = "
                + "(select sum(t1.k1) from t1 where t0.k1 = "
                + "(select sum(t2.k1) from t2 where t1.id = t2.v2))";
        checkAnalyze(sql5);
    }

    @Test
    public void inScalarTest() {
        String sql = "select * from t0 where t0.id in "
                + "(select t1.k1 from t1 where t1.k1 = "
                + "(select sum(t2.id) from t2 where t1.id = t2.id));";
        checkAnalyze(sql);

        String sql1 = "select * from t0 where t0.k1 in ("
                + "select t1.k1 from t1 where "
                + "t1.v2 in (select t2.k1 from t2 where t2.k1 = 3) "
                + "and t1.v2 > (select sum(t2.k1) from t2 where t2.v2 = t1.id))";
        checkAnalyze(sql1);
    }
}
