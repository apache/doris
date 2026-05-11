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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.AlterUserInfo;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class AlterUserStmtTest {

    @Before
    public void setUp() {
        ConnectContext ctx = new ConnectContext();
        ctx.setRemoteIP("192.168.1.1");
        UserIdentity currentUserIdentity = new UserIdentity("root", "192.168.1.1");
        currentUserIdentity.setIsAnalyzed();
        ctx.setCurrentUserIdentity(currentUserIdentity);
        ctx.setThreadLocalInfo();
    }

    private void mockValidateEnv(@Mocked Env env, @Mocked AccessControllerManager accessManager)
            throws UserException {
        new Expectations() {
            {
                Env.getCurrentEnv();
                result = env;
                minTimes = 0;

                env.getAccessManager();
                result = accessManager;
                minTimes = 0;

                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.GRANT);
                result = true;
                minTimes = 0;
            }
        };
    }

    @Test
    public void testTlsRequireNoneOnly(@Mocked Env env,
            @Mocked AccessControllerManager accessManager) throws UserException {
        mockValidateEnv(env, accessManager);

        AlterUserInfo info = new AlterUserInfo(false, new UserDesc(new UserIdentity("tls_user", "%")),
                PasswordOptions.UNSET_OPTION, null, TlsOptions.requireNone());
        info.validate();

        Assert.assertEquals(org.apache.doris.alter.AlterUserOpType.SET_TLS_REQUIRE, info.getOpType());
        Assert.assertFalse(info.getUserIdent().hasTlsRequirements());
    }

    @Test
    public void testTlsRequireSanOnly(@Mocked Env env,
            @Mocked AccessControllerManager accessManager) throws UserException {
        mockValidateEnv(env, accessManager);

        TlsOptions tlsOptions = TlsOptions.of(Collections.singletonList(Pair.of("SAN", "DNS:example.com")));
        AlterUserInfo info = new AlterUserInfo(false, new UserDesc(new UserIdentity("tls_user", "%")),
                PasswordOptions.UNSET_OPTION, null, tlsOptions);
        info.validate();

        Assert.assertEquals(org.apache.doris.alter.AlterUserOpType.SET_TLS_REQUIRE, info.getOpType());
        Assert.assertEquals("DNS:example.com", info.getUserIdent().getSan());
    }

    @Test(expected = AnalysisException.class)
    public void testTlsWithPasswordChangeNotAllowed(@Mocked Env env,
            @Mocked AccessControllerManager accessManager) throws UserException {
        mockValidateEnv(env, accessManager);

        TlsOptions tlsOptions = TlsOptions.of(Collections.singletonList(Pair.of("SAN", "DNS:example.com")));
        AlterUserInfo info = new AlterUserInfo(false,
                new UserDesc(new UserIdentity("tls_user", "%"), "passwd", true),
                PasswordOptions.UNSET_OPTION, null, tlsOptions);
        info.validate();
    }

    @Test(expected = AnalysisException.class)
    public void testTlsRequireSanEmptyValue(@Mocked Env env,
            @Mocked AccessControllerManager accessManager) throws UserException {
        mockValidateEnv(env, accessManager);
        AlterUserInfo info = new AlterUserInfo(false, new UserDesc(new UserIdentity("tls_user", "%")),
                PasswordOptions.UNSET_OPTION, null,
                TlsOptions.of(Collections.singletonList(Pair.of("SAN", ""))));
        info.validate();
    }

    @Test(expected = AnalysisException.class)
    public void testTlsUnsupportedOption(@Mocked Env env,
            @Mocked AccessControllerManager accessManager) throws UserException {
        mockValidateEnv(env, accessManager);
        AlterUserInfo info = new AlterUserInfo(false, new UserDesc(new UserIdentity("tls_user", "%")),
                PasswordOptions.UNSET_OPTION, null,
                TlsOptions.of(Collections.singletonList(Pair.of("ISSUER", "ca"))));
        info.validate();
    }

    @Test(expected = AnalysisException.class)
    public void testMultipleNonTlsOpsAreRejected(@Mocked Env env,
            @Mocked AccessControllerManager accessManager) throws UserException {
        mockValidateEnv(env, accessManager);
        AlterUserInfo info = new AlterUserInfo(false,
                new UserDesc(new UserIdentity("tls_user", "%"), "passwd", true), PasswordOptions.UNSET_OPTION,
                "new comment", TlsOptions.notSpecified());
        info.validate();
    }

    @Test(expected = AnalysisException.class)
    public void testNoOpsAreRejected(@Mocked Env env,
            @Mocked AccessControllerManager accessManager) throws UserException {
        mockValidateEnv(env, accessManager);
        AlterUserInfo info = new AlterUserInfo(false, new UserDesc(new UserIdentity("tls_user", "%")),
                PasswordOptions.UNSET_OPTION, null, TlsOptions.notSpecified());
        info.validate();
    }
}
