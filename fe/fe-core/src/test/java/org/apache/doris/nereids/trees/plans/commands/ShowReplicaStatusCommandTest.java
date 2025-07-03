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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableRefInfo;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class ShowReplicaStatusCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager accessControllerManager;
    @Mocked
    private ConnectContext connectContext;

    private void runBefore() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testValidate() throws AnalysisException {
        runBefore();
        TableNameInfo tableNameInfo = new TableNameInfo(internalCtl, "test_db", "test_tbl");
        List<String> partitionNames = new ArrayList<>();
        partitionNames.add("p1");
        partitionNames.add("p2");
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false, partitionNames);
        TableRefInfo tableRefInfo = new TableRefInfo(tableNameInfo, null, null, partitionNamesInfo, null, null, null, null);

        //test where is null
        ShowReplicaStatusCommand command = new ShowReplicaStatusCommand(tableRefInfo, null);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        //test where is Equalto
        Expression where = new EqualTo(new UnboundSlot(Lists.newArrayList("status")),
                new StringLiteral("VERSION_ERROR"));
        ShowReplicaStatusCommand command2 = new ShowReplicaStatusCommand(tableRefInfo, where);
        Assertions.assertDoesNotThrow(() -> command2.validate(connectContext));

        //test where is Not, child(0) is EqualTo
        Expression equalTo = new EqualTo(new UnboundSlot(Lists.newArrayList("status")),
                new StringLiteral("OK"));
        Expression where2 = new Not(equalTo);
        ShowReplicaStatusCommand command3 = new ShowReplicaStatusCommand(tableRefInfo, where2);
        Assertions.assertDoesNotThrow(() -> command3.validate(connectContext));

        //test status is wrong
        Expression where3 = new EqualTo(new UnboundSlot(Lists.newArrayList("status")),
                new StringLiteral("test_status"));
        ShowReplicaStatusCommand command4 = new ShowReplicaStatusCommand(tableRefInfo, where3);
        Assertions.assertThrows(AnalysisException.class, () -> command4.validate(connectContext));

        //test catalog is hive
        TableNameInfo tableNameInfo2 = new TableNameInfo("hive", "test_db", "test_tbl");
        TableRefInfo tableRefInfo2 = new TableRefInfo(tableNameInfo2, null, null, partitionNamesInfo, null, null, null, null);
        ShowReplicaStatusCommand command5 = new ShowReplicaStatusCommand(tableRefInfo2, null);
        Assertions.assertThrows(AnalysisException.class, () -> command5.validate(connectContext));
    }
}
