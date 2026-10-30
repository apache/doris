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

package org.apache.doris.qe;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.protocol.MysqlProtocolAdapter;
import org.apache.doris.nereids.trees.plans.commands.SuUserCommand;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class MysqlConnectProcessorSuTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("su_change_tenant");
        createDatabase("su_change_personal");
        addUser("su_change_svc", false);
        addUser("su_change_person", false);
        addUser("su_change_login", false);
        createRole("su_change_role");
        grantPriv("GRANT PROXY_PRIV ON *.*.* TO 'su_change_svc'@'%'");
        grantPriv("GRANT SELECT_PRIV ON internal.su_change_tenant.* TO ROLE 'su_change_role'");
        grantRole("GRANT 'su_change_role' TO 'su_change_person'@'%'");
        grantPriv("GRANT SELECT_PRIV ON internal.su_change_personal.* TO 'su_change_person'@'%'");
        grantPriv("GRANT SELECT_PRIV ON internal.su_change_personal.* TO 'su_change_login'@'%'");
    }

    private ConnectContext switchedContext() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("su_change_svc", "%"));
        ctx.setRemoteIP("127.0.0.1");
        MysqlProtocolAdapter.of(ctx).setHandshakePacket(new MysqlHandshakePacket(0));
        ctx.setThreadLocalInfo();
        new SuUserCommand(new UserIdentity("su_change_person", "%"),
                Collections.singletonList("su_change_role"), null).run(ctx, null);
        return ctx;
    }

    private boolean canSelect(ConnectContext ctx, String db) {
        return Env.getCurrentEnv().getAccessManager().checkDbPriv(ctx.getCurrentUserIdentity(),
                InternalCatalog.INTERNAL_CATALOG_NAME, db, PrivPredicate.SELECT);
    }

    private void changeUser(ConnectContext ctx, String user) throws Exception {
        // COM_CHANGE_USER payload, with an empty native password and no default database.
        ByteBuffer packet = ByteBuffer.allocate(256).order(ByteOrder.LITTLE_ENDIAN);
        packet.put(user.getBytes(StandardCharsets.UTF_8)).put((byte) 0);
        packet.put((byte) 0);
        packet.put((byte) 0);
        packet.putShort((short) 33);
        packet.put(MysqlHandshakePacket.AUTH_PLUGIN_NAME.getBytes(StandardCharsets.UTF_8)).put((byte) 0);
        packet.flip();
        MysqlConnectProcessor processor = new MysqlConnectProcessor(ctx);
        Field packetField = MysqlConnectProcessor.class.getDeclaredField("packetBuf");
        packetField.setAccessible(true);
        packetField.set(processor, packet);
        Method handler = MysqlConnectProcessor.class.getDeclaredMethod("handleChangeUser");
        handler.setAccessible(true);
        handler.invoke(processor);
    }

    private void assertStillNarrowed(ConnectContext ctx) {
        Assertions.assertEquals("su_change_person", ctx.getQualifiedUser());
        Assertions.assertTrue(canSelect(ctx, "su_change_tenant"));
        Assertions.assertFalse(canSelect(ctx, "su_change_personal"));
        Assertions.assertEquals("su_change_svc", ctx.getAuthenticatedIdentity().getQualifiedUser());
        Assertions.assertEquals(Collections.singleton("su_change_role"), ctx.getSessionRoleOverride());
        Assertions.assertFalse(ctx.isKilled());
    }

    @Test
    public void testForbiddenChangeUserPreservesNarrowing() throws Exception {
        try {
            for (String user : new String[] {"root", "admin"}) {
                ConnectContext ctx = switchedContext();
                changeUser(ctx, user);
                Assertions.assertEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
                assertStillNarrowed(ctx);
            }
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testFailedAuthenticationPreservesNarrowing() throws Exception {
        try {
            ConnectContext ctx = switchedContext();
            changeUser(ctx, "su_change_nonexistent");
            Assertions.assertEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
            assertStillNarrowed(ctx);
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testSuccessfulChangeUserClearsNarrowing() throws Exception {
        try {
            ConnectContext ctx = switchedContext();
            changeUser(ctx, "su_change_login");
            Assertions.assertEquals(QueryState.MysqlStateType.OK, ctx.getState().getStateType());
            Assertions.assertEquals("su_change_login", ctx.getQualifiedUser());
            Assertions.assertNull(ctx.getAuthenticatedIdentity());
            Assertions.assertNull(ctx.getSessionRoleOverride());
            Assertions.assertTrue(canSelect(ctx, "su_change_personal"));
            Assertions.assertFalse(canSelect(ctx, "su_change_tenant"));
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testConnectionResetRestoresAuthenticatedIdentity() throws Exception {
        try {
            ConnectContext ctx = switchedContext();
            ctx.resetConnection();
            Assertions.assertEquals("su_change_svc", ctx.getQualifiedUser());
            Assertions.assertNull(ctx.getAuthenticatedIdentity());
            Assertions.assertNull(ctx.getSessionRoleOverride());
            Assertions.assertFalse(canSelect(ctx, "su_change_personal"));
            Assertions.assertFalse(canSelect(ctx, "su_change_tenant"));
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }
}
