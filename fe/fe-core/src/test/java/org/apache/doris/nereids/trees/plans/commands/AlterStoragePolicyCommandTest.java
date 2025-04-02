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
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.policy.PolicyMgr;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class AlterStoragePolicyCommandTest {

    private static final String policyName = "test_policy";

    @Mocked
    private Env env;
    @Mocked
    private ConnectContext ctx;
    @Mocked
    private AccessControllerManager accessManager;
    @Mocked
    private PolicyMgr policyMgr;
    @Mocked
    private StoragePolicy mockPolicy;

    @Test
    public void testValidate_success() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("cooldown_ttl", "86400");

        new Expectations() {
            {
                Env.getCurrentEnv();
                result = env;
                env.getAccessManager();
                result = accessManager;
                accessManager.checkGlobalPriv(ctx, PrivPredicate.ADMIN);
                result = true;

                env.getPolicyMgr();
                result = policyMgr;
                policyMgr.findPolicy(policyName, PolicyTypeEnum.ROW);
                result = Optional.empty();
                policyMgr.getCopiedPoliciesByType(PolicyTypeEnum.STORAGE);
                result = Collections.singletonList(mockPolicy);
                mockPolicy.getPolicyName();
                result = policyName;
                mockPolicy.checkProperties(props);
            }
        };

        AlterStoragePolicyCommand command = new AlterStoragePolicyCommand(policyName, props);
        Assertions.assertDoesNotThrow(() -> command.doRun(ctx, null));
    }

    @Test
    public void testValidate_unknownPolicy() {
        Map<String, String> props = new HashMap<>();
        props.put("some_key", "value");

        new Expectations() {
            {
                Env.getCurrentEnv();
                result = env;
                env.getAccessManager();
                result = accessManager;
                accessManager.checkGlobalPriv(ctx, PrivPredicate.ADMIN);
                result = true;

                env.getPolicyMgr();
                result = policyMgr;
                policyMgr.findPolicy(policyName, PolicyTypeEnum.ROW);
                result = Optional.empty();
                policyMgr.getCopiedPoliciesByType(PolicyTypeEnum.STORAGE);
                result = new ArrayList<>();
            }
        };

        AlterStoragePolicyCommand command = new AlterStoragePolicyCommand(policyName, props);
        Assertions.assertThrows(AnalysisException.class, () -> command.doRun(ctx, null));
    }

    @Test
    public void testValidate_nullProps() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                result = env;
                env.getAccessManager();
                result = accessManager;
                accessManager.checkGlobalPriv(ctx, PrivPredicate.ADMIN);
                result = true;
            }
        };

        AlterStoragePolicyCommand command = new AlterStoragePolicyCommand(policyName, null);
        Assertions.assertThrows(AnalysisException.class, () -> command.doRun(ctx, null));
    }

    @Test
    public void testValidate_rejectStorageResourceChange() {
        Map<String, String> props = new HashMap<>();
        props.put("storage_resource", "s3_resource");

        new Expectations() {
            {
                Env.getCurrentEnv();
                result = env;
                env.getAccessManager();
                result = accessManager;
                accessManager.checkGlobalPriv(ctx, PrivPredicate.ADMIN);
                result = true;

                env.getPolicyMgr();
                result = policyMgr;
                policyMgr.findPolicy(policyName, PolicyTypeEnum.ROW);
                result = Optional.empty();
                policyMgr.getCopiedPoliciesByType(PolicyTypeEnum.STORAGE);
                result = Collections.singletonList(mockPolicy);
                mockPolicy.getPolicyName();
                result = policyName;
            }
        };

        AlterStoragePolicyCommand command = new AlterStoragePolicyCommand(policyName, props);
        Assertions.assertThrows(AnalysisException.class, () -> command.doRun(ctx, null));
    }
}
