// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.mysql;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.UserResource;
import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.qe.ConnectContext;
import com.baidu.palo.system.SystemInfoService;
import com.google.common.base.Strings;

// MySQL protocol util
public class MysqlProto {
    private static final Logger LOG = LogManager.getLogger(MysqlProto.class);

    // scramble: data receive from server.
    // randomString: data send by server in plugin data field
    private static boolean authenticate(ConnectContext context, byte[] scramble, byte[] randomString, String user) {
        String usePass = scramble.length == 0 ? "NO" : "YES";
        String clusterName = "";
        
        if (user == null || user.isEmpty()) {
            ErrorReport.report(ErrorCode.ERR_ACCESS_DENIED_ERROR, "", usePass);
            return false;
        }
        String remoteIp = "";
        // check ip
        if (user.charAt(0) == '@') {
            String[] strList =  user.split("@", 3);
            if (strList.length != 3) {
                ErrorReport.report(ErrorCode.ERR_ACCESS_DENIED_ERROR, strList[1]);
                return false;
            }
            remoteIp = strList[1];
            user = strList[2];
        }

        // check deploy id
        String[] strList = user.split("@", 2);
        if (strList.length > 1) {
            user = strList[0];
            clusterName = strList[1];
            try {
                if (Catalog.getInstance().getCluster(clusterName) == null 
                        && Integer.valueOf(strList[1]) != context.getCatalog().getClusterId()) {
                    ErrorReport.report(ErrorCode.ERR_UNKNOWN_CLUSTER_ID, strList[1]);
                    return false;
                }
            } catch (Throwable e) {
                ErrorReport.report(ErrorCode.ERR_UNKNOWN_CLUSTER_ID, strList[1]);
                return false;
            }
        }

        strList = user.split("#", 2);
        if (strList.length > 1) {
            user = strList[0];
            if (UserResource.isValidGroup(strList[1])) {
                context.getSessionVariable().setResourceGroup(strList[1]);
            }
        }
        
        if (Strings.isNullOrEmpty(clusterName)) {
            clusterName = SystemInfoService.DEFAULT_CLUSTER;   
        }
        context.setCluster(clusterName);
        
        if (Catalog.getInstance().getUserMgr().getPassword(user) == null) {
            user = ClusterNamespace.getFullName(clusterName, user);
        }
        
        byte[] userPassword = Catalog.getInstance().getUserMgr().getPassword(user);
        
        if (userPassword == null) {
            ErrorReport.report(ErrorCode.ERR_ACCESS_DENIED_ERROR, user, usePass);
            return false;
        }

        userPassword = MysqlPassword.getSaltFromPassword(userPassword);

        // when the length of password is zero, the user has no password
        if ((scramble.length == userPassword.length)
                && (scramble.length == 0 || MysqlPassword.checkScramble(scramble, randomString, userPassword))) {
            // authenticate success
            context.setUser(user);
        } else {
            // password check failed.
            ErrorReport.report(ErrorCode.ERR_ACCESS_DENIED_ERROR, user, usePass);
            return false;
        }
       
        // check whitelist
        if (remoteIp.equals("")) {
            remoteIp = context.getMysqlChannel().getRemoteIp();
        }
        boolean ok = context.getCatalog().checkWhiteList(user, remoteIp);
        if (!ok) {
            LOG.warn("you are deny by whiltList remoteIp={} user={}",
                    context.getMysqlChannel().getRemoteIp(), user);
            ErrorReport.report(ErrorCode.ERR_ACCESS_DENIED_ERROR, user, usePass);
            return false;
        }

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
                Catalog.getInstance().changeDb(context, dbFullName);
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
