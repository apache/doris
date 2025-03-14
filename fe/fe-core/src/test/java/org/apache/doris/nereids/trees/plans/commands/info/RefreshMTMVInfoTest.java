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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.Env;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RefreshMTMVInfoTest {

    @Mocked
    private ConnectContext ctx;
    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager accessControllerManager;

    private String ctlName = "ctl1";
    private String dbName = "db1";
    private String hasPrivTableName = "t1";
    private String noPrivTableName = "t2";

    @Test
    void testAuth() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                accessControllerManager.checkTblPriv((ConnectContext) any, ctlName, dbName, hasPrivTableName,
                        PrivPredicate.CREATE);
                minTimes = 0;
                result = true;

                accessControllerManager.checkTblPriv((ConnectContext) any, ctlName, dbName, noPrivTableName,
                        PrivPredicate.CREATE);
                minTimes = 0;
                result = false;
            }
        };
        RefreshMTMVInfo hasPrivInfo = new RefreshMTMVInfo(new TableNameInfo(ctlName, dbName, hasPrivTableName),
                Lists.newArrayList(),
                true);
        hasPrivInfo.checkPriv(ctx);
        RefreshMTMVInfo noPrivInfo = new RefreshMTMVInfo(new TableNameInfo(ctlName, dbName, noPrivTableName),
                Lists.newArrayList(),
                true);
        Assertions.assertThrows(AnalysisException.class, () -> noPrivInfo.checkPriv(ctx));
    }
}

