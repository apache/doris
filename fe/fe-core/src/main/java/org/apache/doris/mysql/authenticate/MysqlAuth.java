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

package org.apache.doris.mysql.authenticate;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlAuthSwitchPacket;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlClearTextPacket;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.authenticate.ldap.LdapAuthenticate;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;


public class MysqlAuth {
    private static final Logger LOG = LogManager.getLogger(MysqlAuth.class);

    // scramble: data receive from server.
    // randomString: data send by server in plugin data field
    // user_name#HIGH@cluster_name
    private static boolean internalAuthenticate(ConnectContext context, byte[] scramble,
            byte[] randomString, String qualifiedUser) {
        String remoteIp = context.getMysqlChannel().getRemoteIp();
        List<UserIdentity> currentUserIdentity = Lists.newArrayList();

        try {
            Env.getCurrentEnv().getAuth().checkPassword(qualifiedUser, remoteIp,
                    scramble, randomString, currentUserIdentity);
        } catch (AuthenticationException e) {
            ErrorReport.report(e.errorCode, e.msgs);
            return false;
        }

        context.setCurrentUserIdentity(currentUserIdentity.get(0));
        context.setRemoteIP(remoteIp);
        return true;
    }

    // Default auth uses doris internal user system to authenticate.
    private static boolean defaultAuth(
            ConnectContext context,
            String qualifiedUser,
            MysqlChannel channel,
            MysqlSerializer serializer,
            MysqlAuthPacket authPacket,
            MysqlHandshakePacket handshakePacket) throws IOException {
        // Starting with MySQL 8.0.4, MySQL changed the default authentication plugin for MySQL client
        // from mysql_native_password to caching_sha2_password.
        // ref: https://mysqlserverteam.com/mysql-8-0-4-new-default-authentication-plugin-caching_sha2_password/
        // So, User use mysql client or ODBC Driver after 8.0.4 have problem to connect to Doris
        // with password.
        // So Doris support the Protocol::AuthSwitchRequest to tell client to keep the default password plugin
        // which Doris is using now.
        // Note: Check the authPacket whether support plugin auth firstly,
        // before we check AuthPlugin between doris and client to compatible with older version: like mysql 5.1
        if (authPacket.getCapability().isPluginAuth()
                && !handshakePacket.checkAuthPluginSameAsDoris(authPacket.getPluginName())) {
            // 1. clear the serializer
            serializer.reset();
            // 2. build the auth switch request and send to the client
            handshakePacket.buildAuthSwitchRequest(serializer);
            channel.sendAndFlush(serializer.toByteBuffer());
            // Server receive auth switch response packet from client.
            ByteBuffer authSwitchResponse = channel.fetchOnePacket();
            if (authSwitchResponse == null) {
                // receive response failed.
                return false;
            }
            // 3. the client use default password plugin of Doris to dispose
            // password
            authPacket.setAuthResponse(MysqlProto.readEofString(authSwitchResponse));
        }

        // NOTE: when we behind proxy, we need random string sent by proxy.
        byte[] randomString = handshakePacket.getAuthPluginData();
        if (Config.proxy_auth_enable && authPacket.getRandomString() != null) {
            randomString = authPacket.getRandomString();
        }
        // check authenticate
        if (!internalAuthenticate(context, authPacket.getAuthResponse(), randomString, qualifiedUser)) {
            MysqlProto.sendResponsePacket(context);
            return false;
        }
        return true;
    }

    /*
     * ldap:
     * server ---AuthSwitch---> client
     * server <--- clear text password --- client
     */
    private static boolean ldapAuth(
            ConnectContext context,
            String qualifiedUser,
            MysqlChannel channel,
            MysqlSerializer serializer) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("user:{} start to ldap authenticate.", qualifiedUser);
        }
        // server send authentication switch packet to request password clear text.
        // https://dev.mysql.com/doc/internals/en/authentication-method-change.html
        serializer.reset();
        MysqlAuthSwitchPacket mysqlAuthSwitchPacket = new MysqlAuthSwitchPacket();
        mysqlAuthSwitchPacket.writeTo(serializer);
        channel.sendAndFlush(serializer.toByteBuffer());

        // Server receive password clear text.
        ByteBuffer authSwitchResponse = channel.fetchOnePacket();
        if (authSwitchResponse == null) {
            return false;
        }
        MysqlClearTextPacket clearTextPacket = new MysqlClearTextPacket();
        if (!clearTextPacket.readFrom(authSwitchResponse)) {
            ErrorReport.report(ErrorCode.ERR_NOT_SUPPORTED_AUTH_MODE);
            MysqlProto.sendResponsePacket(context);
            return false;
        }
        if (!LdapAuthenticate.authenticate(context, clearTextPacket.getPassword(), qualifiedUser)) {
            MysqlProto.sendResponsePacket(context);
            return false;
        }
        return true;
    }

    // Based on FE configuration and some prerequisites, decide which authentication type to actually use
    private static MysqlAuthType useWhichAuthType(ConnectContext context, String qualifiedUser) throws IOException {
        MysqlAuthType typeConfig = MysqlAuthType.getAuthTypeConfig();

        // Root and admin are internal users of the Doris.
        // They are used to set the ldap admin password.
        // Cannot use external authentication.
        if (qualifiedUser.equals(Auth.ROOT_USER) || qualifiedUser.equals(Auth.ADMIN_USER)) {
            return MysqlAuthType.DEFAULT;
        }

        // precondition
        switch (typeConfig) {
            case LDAP:
                try {
                    // If LDAP authentication is enabled and the user exists in LDAP, use LDAP authentication,
                    // otherwise use Doris internal authentication.
                    if (!Env.getCurrentEnv().getAuth().getLdapManager().doesUserExist(qualifiedUser)) {
                        return MysqlAuthType.DEFAULT;
                    }
                } catch (Exception e) {
                    // TODO: can we catch exception here？
                    LOG.warn("Check if user exists in ldap error.", e);
                    MysqlProto.sendResponsePacket(context);
                    return MysqlAuthType.DEFAULT;
                }
                break;
            default:
        }
        return typeConfig;
    }

    public static boolean authenticate(
            ConnectContext context,
            String qualifiedUser,
            MysqlChannel channel,
            MysqlSerializer serializer,
            MysqlAuthPacket authPacket,
            MysqlHandshakePacket handshakePacket) throws IOException {
        MysqlAuthType authType = useWhichAuthType(context, qualifiedUser);
        switch (authType) {
            case DEFAULT:
                return defaultAuth(context, qualifiedUser, channel, serializer, authPacket, handshakePacket);
            case LDAP:
                return ldapAuth(context, qualifiedUser, channel, serializer);
            default:
        }
        return false;
    }
}
