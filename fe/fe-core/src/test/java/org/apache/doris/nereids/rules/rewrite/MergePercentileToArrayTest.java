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

public class MergePercentileToArrayTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("merge_percentile_to_array");
        createTable(
                "create table merge_percentile_to_array.t (\n"
                        + "pk int, a int, b int\n"
                        + ")\n"
                        + "distributed by hash(pk) buckets 10\n"
                        + "properties('replication_num' = '1');"
        );
        connectContext.setDatabase("default_cluster:merge_percentile_to_array");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void eliminateMax() {
        String sql = "select sum(a), percentile(pk, 0.1) as c1, percentile(pk, 0.2) as c2 from t group by b;";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        logicalProject(logicalAggregate(any())).when(p ->
                            p.getProjects().get(1).toSql().contains("element_at(percentile_array")
                                && p.getProjects().get(2).toSql().contains("element_at(percentile_array"))
                );
    }
}

