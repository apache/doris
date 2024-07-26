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

import org.apache.doris.common.ConnectionException;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.xnio.StreamConnection;
import org.xnio.channels.Channels;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

/**
 * This class used to read/write MySQL logical packet.
 * MySQL protocol will split one logical packet more than 16MB to many packets.
 * http://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
 */
public class MysqlChannel implements BytesChannel {
    // logger for this class
    private static final Logger LOG = LogManager.getLogger(MysqlChannel.class);
    // max length which one MySQL physical can hold, if one logical packet is bigger than this,
    // one packet will split to many packets
    public static final int MAX_PHYSICAL_PACKET_LENGTH = 0xffffff;
    // MySQL packet header length
    protected static final int PACKET_HEADER_LEN = 4;
    // SSL packet header length
    protected static final int SSL_PACKET_HEADER_LEN = 5;
    // next sequence id to receive or send
    protected int sequenceId;
    // channel connected with client
    private StreamConnection conn;
    // used to receive/send header, avoiding new this many time.
    protected ByteBuffer headerByteBuffer;
    protected ByteBuffer defaultBuffer;
    protected ByteBuffer sslHeaderByteBuffer;
    protected ByteBuffer tempBuffer;
    protected ByteBuffer remainingBuffer;
    protected ByteBuffer sendBuffer;

    protected ByteBuffer decryptAppData;
    protected ByteBuffer encryptNetData;

    // for log and show
    protected String remoteHostPortString;
    protected String remoteIp;
    protected boolean isSend;

    protected boolean isSslMode;
    protected boolean isSslHandshaking;
    private SSLEngine sslEngine;

    protected volatile MysqlSerializer serializer = MysqlSerializer.newInstance();

    // mysql flag CLIENT_DEPRECATE_EOF
    private boolean clientDeprecatedEOF;

    private ConnectContext context;

    protected MysqlChannel() {
        // For DummyMysqlChannel
    }

    public void setClientDeprecatedEOF() {
        clientDeprecatedEOF = true;
    }

    public boolean clientDeprecatedEOF() {
        return clientDeprecatedEOF;
    }

    public MysqlChannel(StreamConnection connection, ConnectContext context) {
        Preconditions.checkNotNull(connection);
        this.sequenceId = 0;
        this.isSend = false;
        this.remoteHostPortString = "";
        this.remoteIp = "";
        this.conn = connection;

        // if proxy protocal is enabled, the remote address will be got from proxy protocal header
        // and overwrite the original remote address.
        if (connection.getPeerAddress() instanceof InetSocketAddress) {
            InetSocketAddress address = (InetSocketAddress) connection.getPeerAddress();
            remoteHostPortString = NetUtils
                    .getHostPortInAccessibleFormat(address.getHostString(), address.getPort());
            remoteIp = address.getAddress().getHostAddress();
        } else {
            // Reach here, what's it?
            remoteHostPortString = connection.getPeerAddress().toString();
            remoteIp = connection.getPeerAddress().toString();
        }
        this.defaultBuffer = ByteBuffer.allocate(16 * 1024);
        this.headerByteBuffer = ByteBuffer.allocate(PACKET_HEADER_LEN);
        this.sendBuffer = ByteBuffer.allocate(2 * 1024 * 1024);
        this.context = context;
    }

    public void initSslBuffer() {
        // allocate buffer when needed.
        this.remainingBuffer = ByteBuffer.allocate(16 * 1024);
        this.remainingBuffer.flip();
        this.tempBuffer = ByteBuffer.allocate(16 * 1024);
        this.sslHeaderByteBuffer = ByteBuffer.allocate(SSL_PACKET_HEADER_LEN);
    }

    public void setSequenceId(int sequenceId) {
        this.sequenceId = sequenceId;
    }

    public String getRemoteIp() {
        return remoteIp;
    }

    public void setSslEngine(SSLEngine sslEngine) {
        this.sslEngine = sslEngine;
        decryptAppData = ByteBuffer.allocate(sslEngine.getSession().getApplicationBufferSize() * 2);
        encryptNetData = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize() * 2);
    }

    public void setSslMode(boolean sslMode) {
        isSslMode = sslMode;
        if (isSslMode) {
            // channel in ssl mode means handshake phase has finished.
            isSslHandshaking = false;
        }
    }

    public void setSslHandshaking(boolean sslHandshaking) {
        isSslHandshaking = sslHandshaking;
    }

    private int packetId() {
        byte[] header = headerByteBuffer.array();
        return header[3] & 0xFF;
    }

    private int packetLen(boolean isSslHeader) {
        if (isSslHeader) {
            byte[] header = sslHeaderByteBuffer.array();
            return (header[4] & 0xFF) | ((header[3] & 0XFF) << 8);
        } else {
            byte[] header = headerByteBuffer.array();
            return (header[0] & 0xFF) | ((header[1] & 0XFF) << 8) | ((header[2] & 0XFF) << 16);
        }
    }

    private void accSequenceId() {
        sequenceId++;
        if (sequenceId > 255) {
            sequenceId = 0;
        }
    }

    // Close channel
    public void close() {
        try {
            conn.close();
        } catch (IOException e) {
            LOG.warn("Close channel exception, ignore.");
        }
    }

    // all packet header is not encrypted, packet body is not sure.
    protected int readAll(ByteBuffer dstBuf, boolean isHeader) throws IOException {
        int readLen = 0;
        if (!dstBuf.hasRemaining()) {
            return 0;
        }
        if (remainingBuffer != null && remainingBuffer.hasRemaining()) {
            int oldLen = dstBuf.position();
            while (dstBuf.hasRemaining()) {
                dstBuf.put(remainingBuffer.get());
            }
            return dstBuf.position() - oldLen;
        }
        try {
            while (dstBuf.remaining() != 0) {
                int ret = Channels.readBlocking(conn.getSourceChannel(), dstBuf, context.getNetReadTimeout(),
                        TimeUnit.SECONDS);
                // return -1 when remote peer close the channel
                if (ret == -1) {
                    decryptData(dstBuf, isHeader);
                    return readLen;
                }
                readLen += ret;
            }
            decryptData(dstBuf, isHeader);
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Read channel exception, ignore.", e);
            }
            return 0;
        }
        return readLen;
    }

    @Override
    public int read(ByteBuffer dstBuf) {
        int readLen = 0;
        try {
            while (dstBuf.remaining() != 0) {
                int ret = Channels.readBlocking(conn.getSourceChannel(), dstBuf, context.getNetReadTimeout(),
                        TimeUnit.SECONDS);
                // return -1 when remote peer close the channel
                if (ret == -1) {
                    return 0;
                }
                readLen += ret;
            }
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Read channel exception, ignore.", e);
            }
            return 0;
        }
        return readLen;
    }

    protected void decryptData(ByteBuffer dstBuf, boolean isHeader) throws SSLException {
        // after decrypt, we get a mysql packet with mysql header.
        if (!isSslMode || isHeader) {
            return;
        }
        dstBuf.flip();
        decryptAppData.clear();
        // unwrap will remove ssl header.
        while (true) {
            SSLEngineResult result = sslEngine.unwrap(dstBuf, decryptAppData);
            if (handleUnwrapResult(result) && !dstBuf.hasRemaining()) {
                break;
            }
            // if BUFFER_OVERFLOW or BUFFER_UNDERFLOW, need to unwrap again, so we do nothing.
        }
        decryptAppData.flip();
        dstBuf.clear();
        dstBuf.put(decryptAppData);
        dstBuf.flip();
    }

    // read one logical mysql protocol packet
    // null for channel is closed.
    // NOTE: all of the following code is assumed that the channel is in block mode.
    // if in handshaking mode we return a packet with header otherwise without header.
    public ByteBuffer fetchOnePacket() throws IOException {
        int readLen;
        ByteBuffer result = defaultBuffer;
        result.clear();

        while (true) {
            int packetLen;
            // one SSL packet may include multiple Mysql packets, we use remainingBuffer to store them.
            if ((isSslMode || isSslHandshaking) && !remainingBuffer.hasRemaining()) {
                if (remainingBuffer.position() != 0) {
                    remainingBuffer.clear();
                    remainingBuffer.flip();
                }
                sslHeaderByteBuffer.clear();
                readLen = readAll(sslHeaderByteBuffer, true);
                if (readLen != SSL_PACKET_HEADER_LEN) {
                    // remote has close this channel
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Receive ssl packet header failed, remote may close the channel.");
                    }
                    return null;
                }
                // when handshaking and ssl mode, sslengine unwrap need a packet with header.
                result.put(sslHeaderByteBuffer.array());
                packetLen = packetLen(true);
            } else {
                headerByteBuffer.clear();
                readLen = readAll(headerByteBuffer, true);
                if (readLen != PACKET_HEADER_LEN) {
                    // remote has close this channel
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Receive packet header failed, remote may close the channel.");
                    }
                    return null;
                }
                if (packetId() != sequenceId) {
                    LOG.warn("receive packet sequence id[" + packetId() + "] want to get[" + sequenceId + "]");
                    throw new IOException("Bad packet sequence.");
                }
                packetLen = packetLen(false);
            }
            result = expandPacket(result, packetLen);

            // read one physical packet
            // before read, set limit to make read only one packet
            result.limit(result.position() + packetLen);
            readLen = readAll(result, false);
            if (isSslMode && remainingBuffer.position() == 0) {
                byte[] header = result.array();
                int packetId = header[3] & 0xFF;
                if (packetId != sequenceId) {
                    LOG.warn("receive packet sequence id[" + packetId() + "] want to get[" + sequenceId + "]");
                    throw new IOException("Bad packet sequence.");
                }
                int mysqlPacketLength = (header[0] & 0xFF) | ((header[1] & 0XFF) << 8) | ((header[2] & 0XFF) << 16);
                // remove mysql packet header
                result.position(4);
                result.compact();
                // when encounter large sql query, one mysql packet will be packed as multiple ssl packets.
                // we need to read all ssl packets to combine the complete mysql packet.
                while (mysqlPacketLength > result.limit()) {
                    sslHeaderByteBuffer.clear();
                    readLen = readAll(sslHeaderByteBuffer, true);
                    if (readLen != SSL_PACKET_HEADER_LEN) {
                        // remote has close this channel
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Receive ssl packet header failed, remote may close the channel.");
                        }
                        return null;
                    }
                    tempBuffer.clear();
                    tempBuffer.put(sslHeaderByteBuffer.array());
                    packetLen = packetLen(true);
                    LOG.info("one ssl packet length is: " + packetLen);
                    tempBuffer = expandPacket(tempBuffer, packetLen);
                    result = expandPacket(result, tempBuffer.capacity());
                    // read one physical packet
                    // before read, set limit to make read only one packet
                    tempBuffer.limit(tempBuffer.position() + packetLen);
                    readLen = readAll(tempBuffer, false);
                    result.put(tempBuffer);
                    result.limit(result.position());
                    LOG.info("result is pos: " + result.position() + ", limit: "
                            + result.limit() + "capacity: " + result.capacity());
                }
                if (mysqlPacketLength < result.position()) {
                    LOG.info("one SSL packet has multiple mysql packets.");
                    LOG.info("mysql packet length is " + mysqlPacketLength + ", result is pos: "
                            + result.position() + ", limit: " + result.limit() + "capacity: " + result.capacity());
                    result.flip();
                    result.position(mysqlPacketLength);
                    remainingBuffer.clear();
                    remainingBuffer.put(result);
                    remainingBuffer.flip();
                }
                result.position(mysqlPacketLength);
            }
            if (readLen != packetLen) {
                LOG.warn("Length of received packet content(" + readLen
                        + ") is not equal with length in head.(" + packetLen + ")");
                return null;
            }
            if (!isSslHandshaking) {
                accSequenceId();
            }
            if (packetLen != MAX_PHYSICAL_PACKET_LENGTH) {
                result.flip();
                break;
            }
        }
        return result;
    }

    @NotNull
    private ByteBuffer expandPacket(ByteBuffer result, int packetLen) {
        if ((result.capacity() - result.position()) < packetLen) {
            // byte buffer is not enough, new one packet
            ByteBuffer tmp;
            if (packetLen < MAX_PHYSICAL_PACKET_LENGTH) {
                // last packet, enough to this packet is OK.
                tmp = ByteBuffer.allocate(packetLen + result.position());
            } else {
                // already have packet, to allocate two packet.
                tmp = ByteBuffer.allocate(2 * packetLen + result.position());
            }
            tmp.put(result.array(), 0, result.position());
            result = tmp;
        }
        result.limit(result.position() + packetLen);
        return result;
    }

    protected void realNetSend(ByteBuffer buffer) throws IOException {
        buffer = encryptData(buffer);
        long bufLen = buffer.remaining();
        long start = System.currentTimeMillis();
        long writeLen = Channels.writeBlocking(conn.getSinkChannel(), buffer, context.getNetWriteTimeout(),
                TimeUnit.SECONDS);
        if (bufLen != writeLen) {
            long duration = System.currentTimeMillis() - start;
            throw new ConnectionException("Write mysql packet failed.[write=" + writeLen
                    + ", needToWrite=" + bufLen + "], duration: " + duration + " ms");
        }
        Channels.flushBlocking(conn.getSinkChannel(), context.getNetWriteTimeout(), TimeUnit.SECONDS);
        isSend = true;
    }

    protected ByteBuffer encryptData(ByteBuffer dstBuf) throws SSLException {
        if (!isSslMode) {
            return dstBuf;
        }
        encryptNetData.clear();
        while (true) {
            SSLEngineResult result = sslEngine.wrap(dstBuf, encryptNetData);
            if (handleWrapResult(result) && !dstBuf.hasRemaining()) {
                break;
            }
        }
        encryptNetData.flip();
        return encryptNetData;
    }

    public void flush() throws IOException {
        if (null == sendBuffer || sendBuffer.position() == 0) {
            // Nothing to send
            return;
        }
        sendBuffer.flip();
        try {
            realNetSend(sendBuffer);
        } finally {
            sendBuffer.clear();
        }
        isSend = true;
    }

    private void writeHeader(int length, boolean isSsl) throws IOException {
        if (null == sendBuffer) {
            return;
        }
        long leftLength = sendBuffer.capacity() - sendBuffer.position();
        if (leftLength < 4) {
            flush();
        }

        long newLen = length;
        for (int i = 0; i < 3; ++i) {
            sendBuffer.put((byte) newLen);
            newLen >>= 8;
        }
        sendBuffer.put((byte) sequenceId);
    }

    private void writeBuffer(ByteBuffer buffer) throws IOException {
        if (null == sendBuffer) {
            return;
        }
        // If too long for buffer, send buffered data.
        if (sendBuffer.remaining() < buffer.remaining()) {
            // Flush data in buffer.
            flush();
        }
        // Send this buffer if large enough
        if (buffer.remaining() > sendBuffer.remaining()) {
            realNetSend(buffer);
            return;
        }
        // Put it to
        sendBuffer.put(buffer);
    }

    public void sendOnePacket(ByteBuffer packet) throws IOException {
        // handshake in packet with header and has encrypted, need to send in ssl format
        // ssl mode in packet no header and no encrypted, need to encrypted and add header and send in ssl format
        int bufLen;
        int oldLimit = packet.limit();
        while (oldLimit - packet.position() >= MAX_PHYSICAL_PACKET_LENGTH) {
            bufLen = MAX_PHYSICAL_PACKET_LENGTH;
            packet.limit(packet.position() + bufLen);
            if (isSslHandshaking) {
                writeBuffer(packet);
            } else {
                writeHeader(bufLen, isSslMode);
                writeBuffer(packet);
                accSequenceId();
            }
        }
        if (isSslHandshaking) {
            packet.limit(oldLimit);
            writeBuffer(packet);
        } else {
            writeHeader(oldLimit - packet.position(), isSslMode);
            packet.limit(oldLimit);
            writeBuffer(packet);
            accSequenceId();
        }
    }

    public void sendOnePacket(Object[] rows) throws IOException {
        ByteBuffer packet;
        serializer.reset();
        for (Object value : rows) {
            byte[] bytes = String.valueOf(value).getBytes();
            serializer.writeVInt(bytes.length);
            serializer.writeBytes(bytes);
        }
        packet = serializer.toByteBuffer();
        sendOnePacket(packet);
    }

    public void sendAndFlush(ByteBuffer packet) throws IOException {
        sendOnePacket(packet);
        flush();
    }

    // Call this function before send query before
    public void reset() {
        isSend = false;
        if (null != sendBuffer) {
            sendBuffer.clear();
        }
    }

    public boolean isSend() {
        return isSend;
    }

    public String getRemoteHostPortString() {
        return remoteHostPortString;
    }

    public void startAcceptQuery(ConnectContext connectContext, ConnectProcessor connectProcessor) {
        conn.getSourceChannel().setReadListener(new ReadListener(connectContext, connectProcessor));
        conn.getSourceChannel().resumeReads();
    }

    public void suspendAcceptQuery() {
        conn.getSourceChannel().suspendReads();
    }

    public void resumeAcceptQuery() {
        conn.getSourceChannel().resumeReads();
    }

    public void stopAcceptQuery() throws IOException {
        conn.getSourceChannel().shutdownReads();
    }

    public MysqlSerializer getSerializer() {
        return serializer;
    }

    private boolean handleWrapResult(SSLEngineResult sslEngineResult) throws SSLException {
        switch (sslEngineResult.getStatus()) {
            // normal status.
            case OK:
                return true;
            case CLOSED:
                sslEngine.closeOutbound();
                return true;
            case BUFFER_OVERFLOW:
                // Could attempt to drain the serverNetData buffer of any already obtained
                // data, but we'll just increase it to the size needed.
                ByteBuffer newBuffer = ByteBuffer.allocate(encryptNetData.capacity() * 2);
                encryptNetData.flip();
                newBuffer.put(encryptNetData);
                encryptNetData = newBuffer;
                // retry the operation.
                return false;
            // when wrap BUFFER_UNDERFLOW and other status will not appear.
            case BUFFER_UNDERFLOW:
            default:
                throw new IllegalStateException("invalid wrap status: " + sslEngineResult.getStatus());
        }
    }

    private boolean handleUnwrapResult(SSLEngineResult sslEngineResult) {
        switch (sslEngineResult.getStatus()) {
            // normal status.
            case OK:
                return true;
            case CLOSED:
                sslEngine.closeOutbound();
                return true;
            case BUFFER_OVERFLOW:
                // Could attempt to drain the clientAppData buffer of any already obtained
                // data, but we'll just increase it to the size needed.
                ByteBuffer newAppBuffer = ByteBuffer.allocate(decryptAppData.capacity() * 2);
                decryptAppData.flip();
                newAppBuffer.put(decryptAppData);
                decryptAppData = newAppBuffer;
                // retry the operation.
                return false;
            case BUFFER_UNDERFLOW:
            default:
                throw new IllegalStateException("invalid wrap status: " + sslEngineResult.getStatus());
        }
    }

    // for proxy protocal only
    public void setRemoteAddr(String ip, int port) {
        this.remoteIp = ip;
        this.remoteHostPortString = NetUtils.getHostPortInAccessibleFormat(ip, port);
    }
}
