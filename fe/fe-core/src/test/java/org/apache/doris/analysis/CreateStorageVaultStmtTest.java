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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.StorageVault.StorageVaultType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class CreateStorageVaultStmtTest {
    private Analyzer analyzer;
    private String vaultName;

    @Before()
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        vaultName = "hdfs";
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testNormal(@Mocked Env env, @Injectable AccessControllerManager accessManager)
            throws UserException {
        new Expectations() {
            {
                env.getAccessManager();
                result = accessManager;
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        Config.cloud_unique_id = "not_empty";
        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", "hdfs");
        CreateStorageVaultStmt stmt = new CreateStorageVaultStmt(true, vaultName, properties);
        stmt.analyze(analyzer);
        Assert.assertEquals(vaultName, stmt.getStorageVaultName());
        Assert.assertEquals(StorageVaultType.HDFS, stmt.getStorageVaultType());
        Assert.assertEquals("CREATE STORAGE VAULT 'hdfs' PROPERTIES(\"type\"  =  \"hdfs\")", stmt.toSql());
        Config.cloud_unique_id = "";

    }

    @Test(expected = AnalysisException.class)
    public void testUnsupportedResourceType(@Mocked Env env, @Injectable AccessControllerManager accessManager)
            throws UserException {
        new Expectations() {
            {
                env.getAccessManager();
                result = accessManager;
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        Config.cloud_unique_id = "not_empty";
        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", "hadoop");
        CreateStorageVaultStmt stmt = new CreateStorageVaultStmt(true, vaultName, properties);
        stmt.analyze(analyzer);
        Config.cloud_unique_id = "";
    }
}
