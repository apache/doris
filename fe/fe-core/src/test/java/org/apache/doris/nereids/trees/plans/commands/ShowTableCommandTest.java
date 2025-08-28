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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.backup.CatalogMocker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowTableCommandTest {
    @Mocked
    private ConnectContext ctx;

    @Test
    public void testValidate() throws Exception {
        new Expectations() {
            {
                ctx.getDatabase();
                minTimes = 0;
                result = CatalogMocker.TEST_DB_NAME;

                ctx.getDefaultCatalog();
                minTimes = 0;
                result = InternalCatalog.INTERNAL_CATALOG_NAME;

                ConnectContext.get();
                minTimes = 0;
                result = ctx;
            }
        };

        ShowTableCommand command = new ShowTableCommand(CatalogMocker.TEST_DB_NAME,
                InternalCatalog.INTERNAL_CATALOG_NAME, false, PlanType.SHOW_TABLES);
        Assertions.assertDoesNotThrow(() -> command.validate(ctx));
    }

    @Test
    void testInvalidate() {
        new Expectations() {
            {
                ctx.getDatabase();
                minTimes = 0;
                result = "";

                ctx.getDefaultCatalog();
                minTimes = 0;
                result = "";

                ConnectContext.get();
                minTimes = 0;
                result = ctx;
            }
        };

        // db is empty
        ShowTableCommand command = new ShowTableCommand("",
                InternalCatalog.INTERNAL_CATALOG_NAME, false, PlanType.SHOW_TABLES);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(ctx));

        // catalog is empty
        ShowTableCommand command2 = new ShowTableCommand(CatalogMocker.TEST_DB_NAME,
                "", false, PlanType.SHOW_TABLES);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(ctx));
    }
}
