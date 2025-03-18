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

import org.apache.doris.analysis.AnalyzeProperties;
import org.apache.doris.backup.CatalogMocker;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AnalyzeTableCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager accessManager;
    @Mocked
    private ConnectContext ctx;

    @Test
    void testCheckAnalyzePrivilege() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessManager;

                accessManager.checkTblPriv(ctx, internalCtl, CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME,
                        PrivPredicate.SELECT);
                minTimes = 0;
                result = true;

                accessManager.checkTblPriv(ctx, internalCtl, CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME,
                        PrivPredicate.SELECT);
                minTimes = 0;
                result = false;
            }
        };
        TableNameInfo tableNameInfo = new TableNameInfo(internalCtl,
                CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false,
                ImmutableList.of(CatalogMocker.TEST_PARTITION1_NAME));
        AnalyzeTableCommand analyzeTableCommand = new AnalyzeTableCommand(tableNameInfo,
                partitionNamesInfo, ImmutableList.of("k1"), AnalyzeProperties.DEFAULT_PROP);
        // normal
        Assertions.assertDoesNotThrow(() -> analyzeTableCommand.checkAnalyzePrivilege(tableNameInfo));

        // no privilege
        TableNameInfo tableNameInfo2 = new TableNameInfo(internalCtl,
                CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME);
        Assertions.assertThrows(AnalysisException.class,
                () -> analyzeTableCommand.checkAnalyzePrivilege(tableNameInfo2),
                "ANALYZE command denied to user 'null'@'null' for table 'test_db: test_tbl2'");
    }
}

