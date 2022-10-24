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

package org.apache.doris.nereids.datasets.tpch;

import org.apache.doris.nereids.trees.expressions.NamedExpressionUtil;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TPCHPlanTreeTest extends TPCHTestBase {

    private static final Path path = Paths.get(
            "src/test/java/org/apache/doris/nereids/datasets/tpch/TPCH_plan_tree");

    @Override
    public void runBeforeEach() throws Exception {
        NamedExpressionUtil.clear();
    }

    @Test
    public void generatePlanTree() throws IOException {
        ImmutableList<String> tpchSql = ImmutableList.<String>builder()
                .add(TPCHUtils.Q1)
                .add(TPCHUtils.Q2)
                .add(TPCHUtils.Q3)
                .add(TPCHUtils.Q4)
                .add(TPCHUtils.Q5)
                .add(TPCHUtils.Q6)
                .add(TPCHUtils.Q7)
                .add(TPCHUtils.Q8)
                .add(TPCHUtils.Q9)
                .add(TPCHUtils.Q10)
                .add(TPCHUtils.Q11)
                .add(TPCHUtils.Q12)
                .add(TPCHUtils.Q13)
                .add(TPCHUtils.Q14)
                // .add(TPCHUtils.Q15)
                .add(TPCHUtils.Q16)
                .add(TPCHUtils.Q17)
                .add(TPCHUtils.Q18)
                .add(TPCHUtils.Q19)
                .add(TPCHUtils.Q20)
                .add(TPCHUtils.Q21)
                // .add(TPCHUtils.Q22)
                .build();

        StringBuilder sb = new StringBuilder();

        for (String sql : tpchSql) {
            String treeString = PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .treeString();
            sb.append(sql).append("\n").append("\n");
            sb.append(treeString).append("\n");
            sb.append("---------------------").append("\n");
        }

        String allSqlTreeString = sb.toString();
        String readAllSqlTreeString = new String(Files.readAllBytes(path));

        Assertions.assertEquals(readAllSqlTreeString, allSqlTreeString);

        // Files.write(path,
        //         allSqlTreeString.getBytes(StandardCharsets.UTF_8),
        //         StandardOpenOption.TRUNCATE_EXISTING);
    }
}
