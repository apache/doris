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

package org.apache.doris.nereids.types;

import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class VarBinaryUnsupportedCollectionTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabaseAndUse("binary_collections");
        createTable("create table source_bytes (id int, encoded string) duplicate key(id) "
                + "distributed by hash(id) buckets 1 properties ('replication_num'='1')");
    }

    @Test
    public void testUnsupportedBinaryCollectionsFailDuringAnalysis() {
        String values = "array(cast(encoded as varbinary), X'', X'0080FF', NULL)";
        for (String expression : new String[] {
                "array_contains(" + values + ", X'0080FF')",
                "array_position(" + values + ", X'0080FF')",
                "countequal(" + values + ", X'0080FF')",
                "array_distinct(" + values + ")",
                "array_remove(" + values + ", X'0080FF')",
                "array_enumerate_uniq(" + values + ")",
                "array_contains_all(" + values + ", " + values + ")",
                "arrays_overlap(" + values + ", " + values + ")",
                "array_union(" + values + ", " + values + ")",
                "array_except(" + values + ", " + values + ")",
                "array_intersect(" + values + ", " + values + ")",
                "collect_set(cast(encoded as varbinary))",
                "collect_set(cast(encoded as varbinary), 2)"}) {
            org.apache.doris.nereids.exceptions.AnalysisException error = Assertions.assertThrows(
                    org.apache.doris.nereids.exceptions.AnalysisException.class,
                    () -> PlanChecker.from(connectContext).analyze("select " + expression + " from source_bytes"),
                    expression);
            Assertions.assertTrue(error.getMessage().contains("does not support VARBINARY"), error.getMessage());
        }
        // Byte-agnostic array construction and element access remain supported.
        PlanChecker.from(connectContext).analyze("select array(cast(encoded as varbinary))[1] from source_bytes");
        PlanChecker.from(connectContext).analyze("select collect_list(cast(encoded as varbinary)) from source_bytes");
    }

}
