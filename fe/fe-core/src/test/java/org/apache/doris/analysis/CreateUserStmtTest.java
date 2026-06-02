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
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.CreateUserInfo;
import org.apache.doris.qe.ConnectContext;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

public class CreateUserStmtTest {

    @Before
    public void setUp() {
        ConnectContext ctx = new ConnectContext();
        ctx.setRemoteIP("192.168.1.1");
        UserIdentity currentUserIdentity = new UserIdentity("root", "192.168.1.1");
        currentUserIdentity.setIsAnalyzed();
        ctx.setCurrentUserIdentity(currentUserIdentity);
        ctx.setThreadLocalInfo();
    }

    private MockedStatic<Env> mockValidateEnv(Env env, Auth auth,
            AccessControllerManager accessManager) {
        MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getAuth()).thenReturn(auth);
        Mockito.when(auth.getUserId(Mockito.anyString())).thenReturn("");
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(accessManager.checkGlobalPriv(Mockito.any(ConnectContext.class), Mockito.eq(PrivPredicate.GRANT)))
                .thenReturn(true);
        return mockedEnv;
    }

    @Test
    public void testPasswordNormalize() throws AnalysisException {
        Env env = Mockito.mock(Env.class);
        Auth auth = Mockito.mock(Auth.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        try (MockedStatic<Env> ignored = mockValidateEnv(env, auth, accessManager)) {
            CreateUserInfo info = new CreateUserInfo(new UserDesc(new UserIdentity("user", "%"), "passwd", true));
            info.validate();
            Assert.assertEquals("user", info.getUserIdent().getQualifiedUser());
            Assert.assertEquals("*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0", new String(info.getPassword()));

            info = new CreateUserInfo(
                    new UserDesc(new UserIdentity("user", "%"),
                            "*59c70da2f3e3a5bdf46b68f5c8b8f25762bccef0", false));
            info.validate();
            Assert.assertEquals("*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0", new String(info.getPassword()));

            info = new CreateUserInfo(new UserDesc(new UserIdentity("user", "%"), "", false));
            info.validate();
            Assert.assertEquals("", new String(info.getPassword()));
        }
    }

    @Test
    public void testTlsRequireNone() throws AnalysisException {
        Env env = Mockito.mock(Env.class);
        Auth auth = Mockito.mock(Auth.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        try (MockedStatic<Env> ignored = mockValidateEnv(env, auth, accessManager)) {
            CreateUserInfo info = new CreateUserInfo(false,
                    new UserDesc(new UserIdentity("tls_user", "%"), "passwd", true),
                    null, null, null, TlsOptions.requireNone());
            info.validate();

            UserIdentity userIdent = info.getUserIdent();
            Assert.assertFalse(userIdent.hasTlsRequirements());
            Assert.assertNull(userIdent.getSan());
        }
    }

    @Test
    public void testTlsRequireSan() throws AnalysisException {
        Env env = Mockito.mock(Env.class);
        Auth auth = Mockito.mock(Auth.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        try (MockedStatic<Env> ignored = mockValidateEnv(env, auth, accessManager)) {
            TlsOptions tlsOptions = TlsOptions.of(Collections.singletonList(Pair.of("SAN", "DNS:example.com")));
            CreateUserInfo info = new CreateUserInfo(false,
                    new UserDesc(new UserIdentity("tls_user", "%"), "passwd", true),
                    null, null, null, tlsOptions);
            info.validate();

            UserIdentity userIdent = info.getUserIdent();
            Assert.assertEquals("DNS:example.com", userIdent.getSan());
        }
    }

    @Test(expected = AnalysisException.class)
    public void testTlsRequireSanEmptyValue() throws AnalysisException {
        Env env = Mockito.mock(Env.class);
        Auth auth = Mockito.mock(Auth.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        try (MockedStatic<Env> ignored = mockValidateEnv(env, auth, accessManager)) {
            CreateUserInfo info = new CreateUserInfo(false,
                    new UserDesc(new UserIdentity("tls_user", "%"), "passwd", true),
                    null, null, null, TlsOptions.of(Collections.singletonList(Pair.of("SAN", ""))));
            info.validate();
        }
    }

    @Test(expected = AnalysisException.class)
    public void testTlsUnsupportedOption() throws AnalysisException {
        Env env = Mockito.mock(Env.class);
        Auth auth = Mockito.mock(Auth.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        try (MockedStatic<Env> ignored = mockValidateEnv(env, auth, accessManager)) {
            CreateUserInfo info = new CreateUserInfo(false,
                    new UserDesc(new UserIdentity("tls_user", "%"), "passwd", true),
                    null, null, null, TlsOptions.of(Collections.singletonList(Pair.of("ISSUER", "ca"))));
            info.validate();
        }
    }

    @Test(expected = AnalysisException.class)
    public void testEmptyUser() throws AnalysisException {
        Env env = Mockito.mock(Env.class);
        Auth auth = Mockito.mock(Auth.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        try (MockedStatic<Env> ignored = mockValidateEnv(env, auth, accessManager)) {
            CreateUserInfo info = new CreateUserInfo(new UserDesc(new UserIdentity("", "%"), "passwd", true));
            info.validate();
        }
    }

    @Test(expected = AnalysisException.class)
    public void testBadPass() throws AnalysisException {
        Env env = Mockito.mock(Env.class);
        Auth auth = Mockito.mock(Auth.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        try (MockedStatic<Env> ignored = mockValidateEnv(env, auth, accessManager)) {
            CreateUserInfo info = new CreateUserInfo(new UserDesc(new UserIdentity("", "%"), "passwd", false));
            info.validate();
        }
    }
}
