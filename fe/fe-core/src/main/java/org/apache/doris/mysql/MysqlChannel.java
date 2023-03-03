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

import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xnio.StreamConnection;
import org.xnio.channels.Channels;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * This class used to read/write MySQL logical packet.
 * MySQL protocol will split one logical packet more than 16MB to many packets.
 * http://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
 */
public class MysqlChannel {
    // logger for this class
    private static final Logger LOG = LogManager.getLogger(MysqlChannel.class);
    // max length which one MySQL physical can hold, if one logical packet is bigger than this,
    // one packet will split to many packets
    public static final int MAX_PHYSICAL_PACKET_LENGTH = 0xffffff;
    // MySQL packet header length
    protected static final int PACKET_HEADER_LEN = 4;
    // next sequence id to receive or send
    protected int sequenceId;
    // channel connected with client
    private StreamConnection conn;
    // used to receive/send header, avoiding new this many time.
    protected ByteBuffer headerByteBuffer;
    protected ByteBuffer defaultBuffer;
    // default packet byte buffer for most packet
    protected ByteBuffer sendBuffer;
    // for log and show
    protected String remoteHostPortString;
    protected String remoteIp;
    protected boolean isSend;
    // Serializer used to pack MySQL packet.
    protected volatile MysqlSerializer serializer;

    protected MysqlChannel() {
        // For DummyMysqlChannel
    }

    public MysqlChannel(StreamConnection connection) {
        Preconditions.checkNotNull(connection);
        this.sequenceId = 0;
        this.isSend = false;
        this.remoteHostPortString = "";
        this.remoteIp = "";
        this.conn = connection;
        if (connection.getPeerAddress() instanceof InetSocketAddress) {
            InetSocketAddress address = (InetSocketAddress) connection.getPeerAddress();
            remoteHostPortString = address.getHostString() + ":" + address.getPort();
            remoteIp = address.getAddress().getHostAddress();
        } else {
            // Reach here, what's it?
            remoteHostPortString = connection.getPeerAddress().toString();
            remoteIp = connection.getPeerAddress().toString();
        }
        // The serializer and buffers should only be created if this is a real MysqlChannel
        this.serializer = MysqlSerializer.newInstance();
        this.defaultBuffer = ByteBuffer.allocate(16 * 1024);
        this.headerByteBuffer = ByteBuffer.allocate(PACKET_HEADER_LEN);
        this.sendBuffer = ByteBuffer.allocate(2 * 1024 * 1024);
    }

    public void setSequenceId(int sequenceId) {
        this.sequenceId = sequenceId;
    }

    public String getRemoteIp() {
        return remoteIp;
    }

    private int packetId() {
        byte[] header = headerByteBuffer.array();
        return header[3] & 0xFF;
    }

    private int packetLen() {
        byte[] header = headerByteBuffer.array();
        return (header[0] & 0xFF) | ((header[1] & 0XFF) << 8) | ((header[2] & 0XFF) << 16);
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

    protected int readAll(ByteBuffer dstBuf) throws IOException {
        int readLen = 0;
        try {
            while (dstBuf.remaining() != 0) {
                int ret = Channels.readBlocking(conn.getSourceChannel(), dstBuf);
                // return -1 when remote peer close the channel
                if (ret == -1) {
                    return readLen;
                }
                readLen += ret;
            }
        } catch (IOException e) {
            LOG.debug("Read channel exception, ignore.", e);
            return 0;
        }
        return readLen;
    }

    // read one logical mysql protocol packet
    // null for channel is closed.
    // NOTE: all of the following code is assumed that the channel is in block mode.
    public ByteBuffer fetchOnePacket() throws IOException {
        int readLen;
        ByteBuffer result = defaultBuffer;
        result.clear();

        while (true) {
            headerByteBuffer.clear();
            readLen = readAll(headerByteBuffer);
            if (readLen != PACKET_HEADER_LEN) {
                // remote has close this channel
                LOG.debug("Receive packet header failed, remote may close the channel.");
                return null;
            }
            if (packetId() != sequenceId) {
                LOG.warn("receive packet sequence id[" + packetId() + "] want to get[" + sequenceId + "]");
                throw new IOException("Bad packet sequence.");
            }
            int packetLen = packetLen();
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

            // read one physical packet
            // before read, set limit to make read only one packet
            result.limit(result.position() + packetLen);
            readLen = readAll(result);
            if (readLen != packetLen) {
                LOG.warn("Length of received packet content(" + readLen
                        + ") is not equal with length in head.(" + packetLen + ")");
                return null;
            }
            accSequenceId();
            if (packetLen != MAX_PHYSICAL_PACKET_LENGTH) {
                result.flip();
                break;
            }
        }
        return result;
    }

    protected void realNetSend(ByteBuffer buffer) throws IOException {
        long bufLen = buffer.remaining();
        long writeLen = Channels.writeBlocking(conn.getSinkChannel(), buffer);
        if (bufLen != writeLen) {
            throw new IOException("Write mysql packet failed.[write=" + writeLen
                    + ", needToWrite=" + bufLen + "]");
        }
        Channels.flushBlocking(conn.getSinkChannel());
        isSend = true;
    }

    public void flush() throws IOException {
        if (null == sendBuffer || sendBuffer.position() == 0) {
            // Nothing to send
            return;
        }
        sendBuffer.flip();
        realNetSend(sendBuffer);
        sendBuffer.clear();
        isSend = true;
    }

    private void writeHeader(int length) throws IOException {
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
        long leftLength = sendBuffer.capacity() - sendBuffer.position();
        // If too long for buffer, send buffered data.
        if (leftLength < buffer.remaining()) {
            // Flush data in buffer.
            flush();
        }
        // Send this buffer if large enough
        if (buffer.remaining() > sendBuffer.capacity()) {
            realNetSend(buffer);
            return;
        }
        // Put it to
        sendBuffer.put(buffer);
    }

    public void sendOnePacket(ByteBuffer packet) throws IOException {
        int bufLen;
        int oldLimit = packet.limit();
        while (oldLimit - packet.position() >= MAX_PHYSICAL_PACKET_LENGTH) {
            bufLen = MAX_PHYSICAL_PACKET_LENGTH;
            packet.limit(packet.position() + bufLen);
            writeHeader(bufLen);
            writeBuffer(packet);
            accSequenceId();
        }
        writeHeader(oldLimit - packet.position());
        packet.limit(oldLimit);
        writeBuffer(packet);
        accSequenceId();
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
}
