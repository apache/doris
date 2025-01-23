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

package org.apache.doris.nereids.rules.expression;

import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

class ExpressionRewriteSqlTest extends SqlTestBase {

    @Test
    public void testSimplifyConflictPredicate() {
        // test in predicate
        List<String> compareSql = Arrays.asList(
                "id > score and not (id > score)",
                "id > score and not (score < id)",
                "id > score and id <= score",
                "id > score and score >= id"
        );
        for (String s : compareSql) {
            String sql = "select " + s + " as col from T1";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .matches(
                            logicalProject().when(project -> project.getProjects().get(0).toString().equals(
                                    "AND[(id#0 > score#1) IS NULL,NULL] AS `col`#2"
                            )));
        }

        compareSql = Arrays.asList(
                "id < score and not (id < score)",
                "id < score and not (score > id)",
                "id < score and id >= score",
                "id < score and score <= id"
        );
        for (String s : compareSql) {
            String sql = "select " + s + " as col from T1";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .matches(
                            logicalProject().when(project -> project.getProjects().get(0).toString().equals(
                                    "AND[(id#0 < score#1) IS NULL,NULL] AS `col`#2"
                            )));
        }

        compareSql = Arrays.asList(
                "id <= score and not (id <= score)",
                "id <= score and not (score >= id)",
                "id <= score and id > score",
                "id <= score and score < id"
        );
        for (String s : compareSql) {
            String sql = "select " + s + " as col from T1";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .matches(
                            logicalProject().when(project -> project.getProjects().get(0).toString().equals(
                                    "AND[(id#0 <= score#1) IS NULL,NULL] AS `col`#2"
                            )));
        }

        compareSql = Arrays.asList(
                "id = score and not (id = score)",
                "id = score and not (score = id)"
        );
        for (String s : compareSql) {
            String sql = "select " + s + " as col from T1";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .matches(
                            logicalProject().when(project -> project.getProjects().get(0).toString().equals(
                                    "AND[(id#0 = score#1) IS NULL,NULL] AS `col`#2"
                            )));
        }

        String sql = "select not(id > score) and (id > score) as col from T1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalProject().when(project -> project.getProjects().get(0).toString().equals(
                                "AND[(id#0 <= score#1) IS NULL,NULL] AS `col`#2"
                        )));

        compareSql = Arrays.asList(
                "id > score or not (id > score)",
                "id > score or not (score < id)",
                "id > score or id <= score",
                "id > score or score >= id"
        );
        for (String s : compareSql) {
            sql = "select " + s + " as col from T1";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .matches(
                            logicalProject().when(project -> project.getProjects().get(0).toString().equals(
                                    "OR[( not (id#0 > score#1) IS NULL),NULL] AS `col`#2"
                            )));
        }

        compareSql = Arrays.asList(
                "id < score or not (id < score)",
                "id < score or not (score > id)",
                "id < score or id >= score",
                "id < score or score <= id"
        );
        for (String s : compareSql) {
            sql = "select " + s + " as col from T1";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .matches(
                            logicalProject().when(project -> project.getProjects().get(0).toString().equals(
                                    "OR[( not (id#0 < score#1) IS NULL),NULL] AS `col`#2"
                            )));
        }

        compareSql = Arrays.asList(
                "id <= score or not (id <= score)",
                "id <= score or not (score >= id)",
                "id <= score or id > score",
                "id <= score or score < id"
        );
        for (String s : compareSql) {
            sql = "select " + s + " as col from T1";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .matches(
                            logicalProject().when(project -> project.getProjects().get(0).toString().equals(
                                    "OR[( not (id#0 <= score#1) IS NULL),NULL] AS `col`#2"
                            )));
        }

        compareSql = Arrays.asList(
                "id = score or not (id = score)",
                "id = score or not (score = id)"
        );
        for (String s : compareSql) {
            sql = "select " + s + " as col from T1";
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .matches(
                            logicalProject().when(project -> project.getProjects().get(0).toString().equals(
                                    "OR[( not (id#0 = score#1) IS NULL),NULL] AS `col`#2"
                            )));
        }

        sql = "select not(id > score) or (id > score) as col from T1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalProject().when(project -> project.getProjects().get(0).toString().equals(
                                "OR[( not (id#0 <= score#1) IS NULL),NULL] AS `col`#2"
                        )));
    }

    @Test
    void testSimplifyRange() {
        String sql = "select id > 6 and id <= 6 and id < 5 as col from T1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalProject().when(project -> project.getProjects().get(0).toString().equals(
                                "AND[id#0 IS NULL,NULL] AS `col`#2"
                        )));
    }

}
