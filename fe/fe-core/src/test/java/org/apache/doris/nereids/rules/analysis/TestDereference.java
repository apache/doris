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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.VariantType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.test.TestExternalCatalog.TestCatalogProvider;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class TestDereference extends TestWithFeService {

    private static final Map<String, Map<String, List<Column>>> CATALOG_META = ImmutableMap.of(
            "t", ImmutableMap.of(
                    "t", ImmutableList.of(
                            new Column("id", PrimitiveType.INT),
                            new Column("t", new VariantType())
                    )
            )
    );

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createCatalog("create catalog t properties("
                + " \"type\"=\"test\","
                + " \"catalog_provider.class\"=\"org.apache.doris.nereids.rules.analysis.TestDereference$CustomCatalogProvider\""
                + ")");
        connectContext.changeDefaultCatalog("t");
        useDatabase("t");
    }

    @Test
    public void testBindPriority() {
        // column
        testBind("select t from t");
        // table.column
        testBind("select t.t from t");
        // db.table.column
        testBind("select t.t.t from t");
        // catalog.db.table.column
        testBind("select t.t.t.t from t");
        // catalog.db.table.column.subColumn
        testBind("select t.t.t.t.t from t");
        // catalog.db.table.column.subColumn.subColumn2
        testBind("select t.t.t.t.t.t from t");
    }

    private void testBind(String sql) {
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite();
    }

    public static class CustomCatalogProvider implements TestCatalogProvider {

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return CATALOG_META;
        }
    }
}
