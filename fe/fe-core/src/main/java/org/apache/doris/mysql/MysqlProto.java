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

package org.apache.doris.mysql;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.UserResource;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

// MySQL protocol util
public class MysqlProto {
    private static final Logger LOG = LogManager.getLogger(MysqlProto.class);

    // scramble: data receive from server.
    // randomString: data send by server in plug-in data field
    // user_name#HIGH@cluster_name
    private static boolean authenticate(ConnectContext context, byte[] scramble, byte[] randomString, String user) {
        String usePasswd = scramble.length == 0 ? "NO" : "YES";
        
        String tmpUser = user;
        if (tmpUser == null || tmpUser.isEmpty()) {
            ErrorReport.report(ErrorCode.ERR_ACCESS_DENIED_ERROR, "", usePasswd);
            return false;
        }

        // check cluster, user name may contains cluster name or cluster id.
        // eg:
        // user_name@cluster_name
        String clusterName = "";
        String[] strList = tmpUser.split("@", 2);
        if (strList.length > 1) {
            tmpUser = strList[0];
            clusterName = strList[1];
            try {
                // if cluster does not exist and it is not a valid cluster id, authenticate failed
                if (Catalog.getCurrentCatalog().getCluster(clusterName) == null 
                        && Integer.valueOf(strList[1]) != context.getCatalog().getClusterId()) {
                    ErrorReport.report(ErrorCode.ERR_UNKNOWN_CLUSTER_ID, strList[1]);
                    return false;
                }
            } catch (Throwable e) {
                ErrorReport.report(ErrorCode.ERR_UNKNOWN_CLUSTER_ID, strList[1]);
                return false;
            }
        }
        if (Strings.isNullOrEmpty(clusterName)) {
            clusterName = SystemInfoService.DEFAULT_CLUSTER;
        }
        context.setCluster(clusterName);

        // check resource group level. user name may contains resource group level.
        // eg:
        // ...@user_name#HIGH
        // set resource group if it is valid, or just ignore it
        strList = tmpUser.split("#", 2);
        if (strList.length > 1) {
            tmpUser = strList[0];
            if (UserResource.isValidGroup(strList[1])) {
                context.getSessionVariable().setResourceGroup(strList[1]);
            }
        }
        
        LOG.debug("parse cluster: {}", clusterName);
        String qualifiedUser = ClusterNamespace.getFullName(clusterName, tmpUser);
        String remoteIp = context.getMysqlChannel().getRemoteIp();

        List<UserIdentity> currentUserIdentity = Lists.newArrayList();
        if (!Catalog.getCurrentCatalog().getAuth().checkPassword(qualifiedUser, remoteIp,
                scramble, randomString, currentUserIdentity)) {
            ErrorReport.report(ErrorCode.ERR_ACCESS_DENIED_ERROR, qualifiedUser, usePasswd);
            return false;
        }
       
        context.setCurrentUserIdentity(currentUserIdentity.get(0));
        context.setQualifiedUser(qualifiedUser);
        context.setRemoteIP(remoteIp);
        return true;
    }

    // send response packet(OK/EOF/ERR).
    // before call this function, should set information in state of ConnectContext
    public static void sendResponsePacket(ConnectContext context) throws IOException {
        MysqlSerializer serializer = context.getSerializer();
        MysqlChannel channel = context.getMysqlChannel();
        MysqlPacket packet = context.getState().toResponsePacket();

        // send response packet to client
        serializer.reset();
        packet.writeTo(serializer);
        channel.sendAndFlush(serializer.toByteBuffer());
    }

    /**
     * negotiate with client, use MySQL protocol
     * server ---handshake---> client
     * server <--- authenticate --- client
     * server --- response(OK/ERR) ---> client
     * Exception:
     * IOException:
     */
    public static boolean negotiate(ConnectContext context) throws IOException {
        MysqlSerializer serializer = context.getSerializer();
        MysqlChannel channel = context.getMysqlChannel();
        context.getState().setOk();

        // Server send handshake packet to client.
        serializer.reset();
        MysqlHandshakePacket handshakePacket = new MysqlHandshakePacket(context.getConnectionId());
        handshakePacket.writeTo(serializer);
        channel.sendAndFlush(serializer.toByteBuffer());

        // Server receive authenticate packet from client.
        ByteBuffer handshakeResponse = channel.fetchOnePacket();
        if (handshakeResponse == null) {
            // receive response failed.
            return false;
        }
        MysqlAuthPacket authPacket = new MysqlAuthPacket();
        if (!authPacket.readFrom(handshakeResponse)) {
            ErrorReport.report(ErrorCode.ERR_NOT_SUPPORTED_AUTH_MODE);
            sendResponsePacket(context);
            return false;
        }

        // check capability
        if (!MysqlCapability.isCompatible(context.getServerCapability(), authPacket.getCapability())) {
            // TODO: client return capability can not support
            ErrorReport.report(ErrorCode.ERR_NOT_SUPPORTED_AUTH_MODE);
            sendResponsePacket(context);
            return false;
        }
        // change the capability of serializer
        context.setCapability(context.getServerCapability());
        serializer.setCapability(context.getCapability());

        // NOTE: when we behind proxy, we need random string sent by proxy.
        byte[] randomString = handshakePacket.getAuthPluginData();
        if (Config.proxy_auth_enable && authPacket.getRandomString() != null) {
            randomString = authPacket.getRandomString();
        }
        // check authenticate
        if (!authenticate(context, authPacket.getAuthResponse(), randomString, authPacket.getUser())) {
            sendResponsePacket(context);
            return false;
        }

        // set database
        String db = authPacket.getDb();
        if (!Strings.isNullOrEmpty(db)) {
            try {
                String dbFullName = ClusterNamespace.getFullName(context.getClusterName(), db);
                Catalog.getCurrentCatalog().changeDb(context, dbFullName);
            } catch (DdlException e) {
                sendResponsePacket(context);
                return false;
            }
        }
        return true;
    }

    public static byte readByte(ByteBuffer buffer) {
        return buffer.get();
    }

    public static int readInt1(ByteBuffer buffer) {
        return readByte(buffer) & 0XFF;
    }

    public static int readInt2(ByteBuffer buffer) {
        return (readByte(buffer) & 0xFF) | ((readByte(buffer) & 0xFF) << 8);
    }

    public static int readInt3(ByteBuffer buffer) {
        return (readByte(buffer) & 0xFF) | ((readByte(buffer) & 0xFF) << 8) | ((readByte(
                buffer) & 0xFF) << 16);
    }

    public static int readInt4(ByteBuffer buffer) {
        return (readByte(buffer) & 0xFF) | ((readByte(buffer) & 0xFF) << 8) | ((readByte(
                buffer) & 0xFF) << 16) | ((readByte(buffer) & 0XFF) << 24);
    }

    public static long readInt6(ByteBuffer buffer) {
        return (readInt4(buffer) & 0XFFFFFFFFL) | (((long) readInt2(buffer)) << 32);
    }

    public static long readInt8(ByteBuffer buffer) {
        return (readInt4(buffer) & 0XFFFFFFFFL) | (((long) readInt4(buffer)) << 32);
    }

    public static long readVInt(ByteBuffer buffer) {
        int b = readInt1(buffer);

        if (b < 251) {
            return b;
        }
        if (b == 252) {
            return readInt2(buffer);
        }
        if (b == 253) {
            return readInt3(buffer);
        }
        if (b == 254) {
            return readInt8(buffer);
        }
        if (b == 251) {
            throw new NullPointerException();
        }
        return 0;
    }

    public static byte[] readFixedString(ByteBuffer buffer, int len) {
        byte[] buf = new byte[len];
        buffer.get(buf);
        return buf;
    }

    public static byte[] readEofString(ByteBuffer buffer) {
        byte[] buf = new byte[buffer.remaining()];
        buffer.get(buf);
        return buf;
    }

    public static byte[] readLenEncodedString(ByteBuffer buffer) {
        long length = readVInt(buffer);
        byte[] buf = new byte[(int) length];
        buffer.get(buf);
        return buf;
    }

    public static byte[] readNulTerminateString(ByteBuffer buffer) {
        int oldPos = buffer.position();
        int nullPos = oldPos;
        for (nullPos = oldPos; nullPos < buffer.limit(); ++nullPos) {
            if (buffer.get(nullPos) == 0) {
                break;
            }
        }
        byte[] buf = new byte[nullPos - oldPos];
        buffer.get(buf);
        // skip null byte.
        buffer.get();
        return buf;
    }

}
