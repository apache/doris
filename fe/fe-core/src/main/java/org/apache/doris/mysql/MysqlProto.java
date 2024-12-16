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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

// MySQL protocol util
public class MysqlProto {
    private static final Logger LOG = LogManager.getLogger(MysqlProto.class);
    public static final boolean SERVER_USE_SSL = Config.enable_ssl;


    private static String parseUser(ConnectContext context, byte[] scramble, String user) {
        String usePasswd = scramble.length == 0 ? "NO" : "YES";

        String tmpUser = user;
        if (tmpUser == null || tmpUser.isEmpty()) {
            ErrorReport.report(ErrorCode.ERR_ACCESS_DENIED_ERROR, "anonym@" + context.getRemoteIP(), usePasswd);
            return null;
        }

        // check workload group level. user name may contains workload group level.
        // eg:
        // ...@user_name#HIGH
        // set workload group if it is valid, or just ignore it
        String[] strList = tmpUser.split("#", 2);
        if (strList.length > 1) {
            tmpUser = strList[0];
        }

        context.setQualifiedUser(tmpUser);
        return tmpUser;
    }

    // send response packet(OK/EOF/ERR).
    // before call this function, should set information in state of ConnectContext
    public static void sendResponsePacket(ConnectContext context) throws IOException {
        MysqlChannel channel = context.getMysqlChannel();
        MysqlSerializer serializer = channel.getSerializer();
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
        MysqlChannel channel = context.getMysqlChannel();
        MysqlSerializer serializer = channel.getSerializer();
        context.getState().setOk();

        // Server send handshake packet to client.
        serializer.reset();
        MysqlHandshakePacket handshakePacket = new MysqlHandshakePacket(context.getConnectionId());
        handshakePacket.writeTo(serializer);
        context.setMysqlHandshakePacket(handshakePacket);
        try {
            channel.sendAndFlush(serializer.toByteBuffer());
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Send and flush channel exception, ignore.", e);
            }
            return false;
        }

        // Server receive request packet from client, we need to determine which request type it is.
        ByteBuffer clientRequestPacket = channel.fetchOnePacket();
        MysqlCapability capability = new MysqlCapability(MysqlProto.readLowestInt4(clientRequestPacket));

        // Server receive SSL connection request packet from client.
        ByteBuffer sslConnectionRequest;
        // Server receive authenticate packet from client.
        ByteBuffer handshakeResponse;

        if (capability.isClientUseSsl()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("client is using ssl connection.");
            }
            // During development, we set SSL mode to true by default.
            if (SERVER_USE_SSL) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("server is also using ssl connection. Will use ssl mode for data exchange.");
                }
                MysqlSslContext mysqlSslContext = context.getMysqlSslContext();
                mysqlSslContext.init();
                channel.initSslBuffer();
                sslConnectionRequest = clientRequestPacket;
                if (sslConnectionRequest == null) {
                    // receive response failed.
                    return false;
                }
                MysqlSslPacket sslPacket = new MysqlSslPacket();
                if (!sslPacket.readFrom(sslConnectionRequest)) {
                    ErrorReport.report(ErrorCode.ERR_NOT_SUPPORTED_AUTH_MODE);
                    sendResponsePacket(context);
                    return false;
                }
                // try to establish ssl connection.
                try {
                    // set channel to handshake mode to process data packet as ssl packet.
                    channel.setSslHandshaking(true);
                    // The ssl handshake phase still uses plaintext.
                    if (!mysqlSslContext.sslExchange(channel)) {
                        ErrorReport.report(ErrorCode.ERR_NOT_SUPPORTED_AUTH_MODE);
                        sendResponsePacket(context);
                        return false;
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                // if the exchange is successful, the channel will switch to ssl communication mode
                // which means all data after this moment will be ciphertext.

                // Set channel mode to ssl mode to handle socket packet in ssl format.
                channel.setSslMode(true);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("switch to ssl mode.");
                }
                handshakeResponse = channel.fetchOnePacket();
            } else {
                handshakeResponse = clientRequestPacket;
            }
        } else {
            handshakeResponse = clientRequestPacket;
        }

        if (handshakeResponse == null) {
            // receive response failed.
            return false;
        }
        if (capability.isDeprecatedEOF()) {
            context.getMysqlChannel().setClientDeprecatedEOF();
        }

        // we do not save client capability to context, so here we save CLIENT_MULTI_STATEMENTS to MysqlChannel
        if (capability.isClientMultiStatements()) {
            context.getMysqlChannel().setClientMultiStatements();
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

        String qualifiedUser = parseUser(context, authPacket.getAuthResponse(), authPacket.getUser());
        if (qualifiedUser == null) {
            sendResponsePacket(context);
            return false;
        }

        //  authenticate
        if (!Env.getCurrentEnv().getAuthenticatorManager()
                .authenticate(context, qualifiedUser, channel, serializer, authPacket, handshakePacket)) {
            return false;
        }

        // set database
        String db = authPacket.getDb();
        if (!Strings.isNullOrEmpty(db)) {
            String catalogName = null;
            String dbName = null;
            String[] dbNames = db.split("\\.");
            if (dbNames.length == 1) {
                dbName = db;
            } else if (dbNames.length == 2) {
                catalogName = dbNames[0];
                dbName = dbNames[1];
            } else if (dbNames.length > 2) {
                context.getState().setError(ErrorCode.ERR_BAD_DB_ERROR, "Only one dot can be in the name: " + db);
                return false;
            }
            String dbFullName = dbName;

            // check catalog and db exists
            if (catalogName != null) {
                CatalogIf catalogIf = context.getEnv().getCatalogMgr().getCatalog(catalogName);
                if (catalogIf == null) {
                    context.getState()
                            .setError(ErrorCode.ERR_BAD_DB_ERROR, ErrorCode.ERR_BAD_DB_ERROR.formatErrorMsg(db));
                    return false;
                }
                if (catalogIf.getDbNullable(dbFullName) == null) {
                    context.getState()
                            .setError(ErrorCode.ERR_BAD_DB_ERROR, ErrorCode.ERR_BAD_DB_ERROR.formatErrorMsg(db));
                    return false;
                }
            }
            try {
                if (catalogName != null) {
                    context.getEnv().changeCatalog(context, catalogName);
                }
                Env.getCurrentEnv().changeDb(context, dbFullName);
            } catch (DdlException e) {
                context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
                sendResponsePacket(context);
                return false;
            }
        }

        // set resource tag if has
        context.setResourceTags(Env.getCurrentEnv().getAuth().getResourceTags(qualifiedUser),
                Env.getCurrentEnv().getAuth().isAllowResourceTagDowngrade(qualifiedUser));
        return true;
    }

    public static byte readByte(ByteBuffer buffer) {
        return buffer.get();
    }

    public static byte readByteAt(ByteBuffer buffer, int index) {
        return buffer.get(index);
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

    public static int readLowestInt4(ByteBuffer buffer) {
        return (readByteAt(buffer, 0) & 0xFF) | ((readByteAt(buffer, 1) & 0xFF) << 8) | ((readByteAt(
                buffer, 2) & 0xFF) << 16) | ((readByteAt(buffer, 3) & 0XFF) << 24);
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
