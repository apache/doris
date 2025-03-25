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
import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowDatabasesCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;

    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager accessManager;
    @Mocked
    private ConnectContext ctx;
    @Mocked
    private InternalCatalog catalog;
    @Mocked
    private CatalogMgr catalogMgr;

    @Test
    void testValidate() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessManager;

                env.getCatalogMgr();
                minTimes = 0;
                result = catalogMgr;

                catalogMgr.getCatalog(anyString);
                minTimes = 0;
                result = catalog;
            }
        };
        EqualTo equalTo = new EqualTo(new UnboundSlot("schema_name"),
                new StringLiteral(CatalogMocker.TEST_DB_NAME));

        // normal
        ShowDatabasesCommand command = new ShowDatabasesCommand(internalCtl, null, equalTo);
        Assertions.assertDoesNotThrow(() -> command.validate(ctx));

        // catalog is null
        ShowDatabasesCommand command2 = new ShowDatabasesCommand(null, null, equalTo);
        Assertions.assertDoesNotThrow(() -> command2.validate(ctx));
    }
}

