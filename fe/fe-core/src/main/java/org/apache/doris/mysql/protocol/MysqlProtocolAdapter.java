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

package org.apache.doris.mysql.protocol;

import org.apache.doris.mysql.MysqlCapability;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCursorFetchCompatibility;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlSslContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.ConnectPoolMgr;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.qe.protocol.ProtocolAdapter;
import org.apache.doris.thrift.TResultSinkType;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The MySQL protocol side of a connection: the channel to the client, the capabilities negotiated
 * with it, the handshake and SSL state, and the COM_STMT_EXECUTE packet being processed.
 *
 * <p>The channel decides what kind of MySQL connection this is: a {@link MysqlChannel} over a live
 * socket for a client, a {@code ProxyMysqlChannel} for the context the master builds to replay a
 * forwarded statement, or a {@code DummyMysqlChannel} for an internal context that never talks to
 * a client.
 */
public class MysqlProtocolAdapter implements ProtocolAdapter {
    private static final String SSL_PROTOCOL = "TLS";

    private final MysqlChannel channel;
    // the protocol capability which server say it can support
    private final MysqlCapability serverCapability = MysqlCapability.DEFAULT_CAPABILITY;
    // the protocol capability after server and client negotiate
    private volatile MysqlCapability capability;
    // This context is used for SSL connection between server and mysql client.
    private final MysqlSslContext sslContext = new MysqlSslContext(SSL_PROTOCOL);
    private MysqlHandshakePacket handshakePacket;
    // The COM_STMT_EXECUTE packet being processed, forwarded as-is when the statement goes to the master.
    private ByteBuffer prepareExecuteBuffer;
    // Whether the current COM_STMT_EXECUTE requested a server-side read-only cursor.
    private boolean cursorFetchRequested;

    public MysqlProtocolAdapter(MysqlChannel channel) {
        this.channel = channel;
    }

    /** The MySQL side of {@code ctx}; throws if the connection speaks another protocol. */
    public static MysqlProtocolAdapter of(ConnectContext ctx) {
        ProtocolAdapter adapter = ctx.getProtocolAdapter();
        if (adapter instanceof MysqlProtocolAdapter) {
            return (MysqlProtocolAdapter) adapter;
        }
        throw new IllegalStateException("not a MySQL connection: " + adapter.type());
    }

    @Override
    public ConnectType type() {
        return ConnectType.MYSQL;
    }

    @Override
    public String remoteHostPortString(ConnectContext ctx) {
        return channel.getRemoteHostPortString();
    }

    @Override
    public TResultSinkType resultSinkType() {
        return TResultSinkType.MYSQL_PROTOCOL;
    }

    @Override
    public ConnectPoolMgr connectPool(ConnectScheduler scheduler) {
        return scheduler.getConnectPoolMgr();
    }

    @Override
    public void afterStatement(ConnectContext ctx) {
        cursorFetchRequested = false;
    }

    @Override
    public void closeConnection(ConnectContext ctx) {
        channel.close();
    }

    public MysqlChannel getChannel() {
        return channel;
    }

    public MysqlCapability getServerCapability() {
        return serverCapability;
    }

    public MysqlCapability getCapability() {
        return capability;
    }

    public void setCapability(MysqlCapability capability) {
        this.capability = capability;
    }

    public MysqlSslContext getSslContext() {
        return sslContext;
    }

    public void setHandshakePacket(MysqlHandshakePacket handshakePacket) {
        this.handshakePacket = handshakePacket;
    }

    public byte[] getAuthPluginData() {
        return handshakePacket == null ? null : handshakePacket.getAuthPluginData();
    }

    public ByteBuffer getPrepareExecuteBuffer() {
        return prepareExecuteBuffer;
    }

    public void setPrepareExecuteBuffer(ByteBuffer prepareExecuteBuffer) {
        this.prepareExecuteBuffer = prepareExecuteBuffer;
    }

    public boolean isCursorFetchRequested() {
        return cursorFetchRequested;
    }

    public void setCursorFetchRequested(boolean cursorFetchRequested) {
        this.cursorFetchRequested = cursorFetchRequested;
    }

    /**
     * Whether the client will consume the first OK packet after the column definitions of the
     * result it asked a cursor for. Connector/J before 9.5 does, while probing whether the cursor
     * was created; Doris never creates one, so the result must carry one more end marker or an
     * empty result would leave the client waiting.
     */
    public boolean clientConsumesCursorMetadataTerminator(ConnectContext ctx) {
        return cursorFetchRequested
                && MysqlCursorFetchCompatibility.resolve(ctx.getConnectAttributes())
                        != MysqlCursorFetchCompatibility.Behavior.STANDARD;
    }

    // The xnio read loop. A command is read and processed while reads are suspended, which is what
    // serializes the commands of one connection.
    public void startAcceptQuery(ConnectContext ctx, ConnectProcessor connectProcessor) {
        channel.startAcceptQuery(ctx, connectProcessor);
    }

    public void suspendAcceptQuery() {
        channel.suspendAcceptQuery();
    }

    public void resumeAcceptQuery() {
        channel.resumeAcceptQuery();
    }

    public void stopAcceptQuery() throws IOException {
        channel.stopAcceptQuery();
    }
}
