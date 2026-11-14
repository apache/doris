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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.AccessContext;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TUserIdentity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class SessionRoleContextTest {
    @Test
    public void testRpcRolesAreCopiedAndNeverPersisted() {
        UserIdentity account = UserIdentity.createAnalyzedUserIdentWithIp("mapped_user", "%");
        Set<String> roles = new HashSet<>(Collections.singleton("mapped_role"));
        ConnectContext session = new ConnectContext();
        session.setCurrentUserIdentity(account);
        session.setAuthenticatedRoles(roles);
        TUserIdentity snapshot = session.getCurrentUserIdentityForRpc();
        roles.clear();
        session.setAuthenticatedRoles(Collections.singleton("next_role"));
        Assertions.assertEquals(Collections.singleton("mapped_role"), snapshot.getAuthenticatedRoles());
        Assertions.assertTrue(account.getAuthenticatedRoles().isEmpty());

        UserIdentity restored = UserIdentity.fromThrift(snapshot);
        snapshot.getAuthenticatedRoles().clear();
        Assertions.assertEquals(Collections.singleton("mapped_role"), restored.getAuthenticatedRoles());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> restored.getAuthenticatedRoles().clear());
        Assertions.assertEquals(account, restored);
        Assertions.assertEquals(account.hashCode(), restored.hashCode());
        String persisted = GsonUtils.GSON.toJson(restored);
        Assertions.assertFalse(persisted.contains("mapped_role"));
        Assertions.assertTrue(GsonUtils.GSON.fromJson(persisted, UserIdentity.class).getAuthenticatedRoles().isEmpty());
        Assertions.assertFalse(account.toThrift().isSetAuthenticatedRoles());
    }

    @Test
    public void testForwardedRolesSurviveAnotherRpc() {
        TUserIdentity forwarded = UserIdentity.createAnalyzedUserIdentWithIp("mapped_user", "%").toThrift();
        forwarded.setAuthenticatedRoles(Collections.singleton("forwarded_role"));
        ConnectContext session = new ConnectContext();
        session.setCurrentUserIdentity(UserIdentity.fromThrift(forwarded));
        Assertions.assertEquals(Collections.singleton("forwarded_role"),
                session.getCurrentUserIdentityForRpc().getAuthenticatedRoles());
        session.setAuthenticatedRoles(Collections.singleton("additional_role"));
        Assertions.assertEquals(Set.of("forwarded_role", "additional_role"),
                session.getCurrentUserIdentityForRpc().getAuthenticatedRoles());
        AccessContext context = UserIdentityAccessContext.of(session.getCurrentUserIdentity(),
                ConnectionAccessContext.of(session));
        Assertions.assertEquals(Set.of("forwarded_role", "additional_role"), AccessTranslation.userIdentityOf(
                AccessTranslation.subjectOf(session.getCurrentUserIdentity()), context).getAuthenticatedRoles());
    }

    @Test
    public void testRpcSessionKeepsRolesSeparateFromRetainedAccount() {
        TUserIdentity request = UserIdentity.createAnalyzedUserIdentWithIp("mapped_user", "%").toThrift();
        request.setAuthenticatedRoles(Collections.singleton("mapped_role"));
        ConnectContext session = new ConnectContext();
        session.setCurrentUserIdentityFromRpc(request);
        Assertions.assertTrue(session.getCurrentUserIdentity().getAuthenticatedRoles().isEmpty());
        Assertions.assertEquals(Collections.singleton("mapped_role"), session.getAuthenticatedRoles());
        Assertions.assertEquals(Collections.singleton("mapped_role"),
                session.getCurrentUserIdentityForRpc().getAuthenticatedRoles());
        session.setCurrentUserIdentityFromRpc(
                UserIdentity.createAnalyzedUserIdentWithIp("mapped_user", "%").toThrift());
        Assertions.assertTrue(session.getAuthenticatedRoles().isEmpty());
        Assertions.assertFalse(session.getCurrentUserIdentityForRpc().isSetAuthenticatedRoles());
    }

    @Test
    public void testRolesStayWithTheirAccountIncludingHostAndDomain() {
        UserIdentity account = UserIdentity.createAnalyzedUserIdentWithIp("mapped_user", "%");
        ConnectContext session = new ConnectContext();
        session.setCurrentUserIdentity(account);
        session.setAuthenticatedRoles(Collections.singleton("mapped_role"));
        for (UserIdentity other : new UserIdentity[] {
                UserIdentity.createAnalyzedUserIdentWithIp("another_user", "%"),
                UserIdentity.createAnalyzedUserIdentWithIp("mapped_user", "127.0.0.1"),
                UserIdentity.createAnalyzedUserIdentWithDomain("mapped_user", "%")}) {
            Assertions.assertTrue(session.getAuthenticatedRolesFor(other).isEmpty());
            AccessContext context = UserIdentityAccessContext.of(account, ConnectionAccessContext.of(session));
            Assertions.assertTrue(AccessTranslation.userIdentityOf(
                    AccessTranslation.subjectOf(other), context).getAuthenticatedRoles().isEmpty());
        }
    }

    @Test
    public void testRoleContextPreservesPluginCircumstances() {
        TUserIdentity request = UserIdentity.createAnalyzedUserIdentWithIp("mapped_user", "%").toThrift();
        request.setAuthenticatedRoles(Collections.singleton("mapped_role"));
        UserIdentity identity = UserIdentity.fromThrift(request);
        AccessContext delegate = new AccessContext() {
            @Override
            public Optional<String> getClientIp() {
                return Optional.of("127.0.0.2");
            }

            @Override
            public Optional<String> getQueryId() {
                return Optional.of("query_id");
            }
        };
        AccessContext context = UserIdentityAccessContext.of(identity, delegate);
        Assertions.assertEquals(delegate.getClientIp(), context.getClientIp());
        Assertions.assertEquals(delegate.getQueryId(), context.getQueryId());
        Assertions.assertSame(identity, AccessTranslation.userIdentityOf(AccessTranslation.subjectOf(identity), context));
        Assertions.assertSame(delegate, UserIdentityAccessContext.of(UserIdentity.ROOT, delegate));
    }
}
