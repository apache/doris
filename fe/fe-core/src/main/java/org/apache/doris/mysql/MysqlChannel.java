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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

/**
 * This class used to read/write MySQL logical packet.
 * MySQL protocol will split one logical packet more than 16MB to many packets.
 * http://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
 */
public class MysqlChannel {
    // max length which one MySQL physical can hold, if one logical packet is bigger than this,
    // one packet will split to many packets
    public static final int MAX_PHYSICAL_PACKET_LENGTH = 0xffffff;
    // MySQL packet header length
    protected static final int PACKET_HEADER_LEN = 4;
    // SSL packet header length
    protected static final int SSL_PACKET_HEADER_LEN = 5;
    // logger for this class
    protected static final Logger LOG = LogManager.getLogger(MysqlChannel.class);
    // next sequence id to receive or send
    protected int sequenceId;
    // channel connected with client
    protected SocketChannel channel;
    // used to receive/send header, avoiding new this many time.
    protected ByteBuffer headerByteBuffer = ByteBuffer.allocate(PACKET_HEADER_LEN);
    // used to receive/send ssl header, avoiding new this many time.
    protected ByteBuffer sslHeaderByteBuffer = ByteBuffer.allocate(SSL_PACKET_HEADER_LEN);
    protected ByteBuffer decryptAppData;
    protected ByteBuffer encryptNetData;
    // default packet byte buffer for most packet
    protected ByteBuffer defaultBuffer = ByteBuffer.allocate(16 * 1024);
    protected ByteBuffer sendBuffer;

    protected ByteBuffer sendSslBuffer;
    // for log and show
    protected String remoteHostPortString;
    protected String remoteIp;
    protected boolean isSend;
    protected boolean isSslMode;
    protected boolean isSslHandshaking;
    private SSLEngine sslEngine;


    protected MysqlChannel() {
        this.sequenceId = 0;
        this.sendBuffer = ByteBuffer.allocate(2 * 1024 * 1024);
        // todo: change ssl packet capacity.
        this.sendSslBuffer = ByteBuffer.allocate(2 * 1024 * 1024);
        this.isSend = false;
        this.remoteHostPortString = "";
        this.remoteIp = "";
    }

    public MysqlChannel(SocketChannel channel) {
        this.sequenceId = 0;
        this.channel = channel;
        this.sendBuffer = ByteBuffer.allocate(2 * 1024 * 1024);
        this.isSend = false;
        this.remoteHostPortString = "";
        this.remoteIp = "";

        if (channel != null) {
            try {
                if (channel.getRemoteAddress() instanceof InetSocketAddress) {
                    InetSocketAddress address = (InetSocketAddress) channel.getRemoteAddress();
                    // avoid calling getHostName() which may trigger a name service reverse lookup
                    remoteHostPortString = address.getHostString() + ":" + address.getPort();
                    remoteIp = address.getAddress().getHostAddress();
                } else if (channel.getRemoteAddress() != null) {
                    // Reach here, what's it?
                    remoteHostPortString = channel.getRemoteAddress().toString();
                    remoteIp = channel.getRemoteAddress().toString();
                }
            } catch (Exception e) {
                LOG.warn("get remote host string failed: ", e);
            }
        }
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
            headerByteBuffer = ByteBuffer.allocate(SSL_PACKET_HEADER_LEN);
        }
    }

    public void setSslHandshaking(boolean sslHandshaking) {
        isSslHandshaking = sslHandshaking;
    }

    private int packetId() {
        byte[] header = headerByteBuffer.array();
        return header[3] & 0xFF;
    }

    private int packetLen() {
        if (isSslMode || isSslHandshaking) {
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
            channel.close();
        } catch (IOException e) {
            LOG.warn("Close channel exception, ignore.");
        }
    }

    // all packet header is not encrypted, packet body is not sure.
    protected int readAll(ByteBuffer dstBuf, boolean isHeader) throws IOException {
        int readLen = 0;
        while (dstBuf.remaining() != 0) {
            int ret = channel.read(dstBuf);
            // return -1 when remote peer close the channel
            if (ret == -1) {
                decryptData(dstBuf, isHeader);
                return readLen;
            }
            readLen += ret;
        }
        // if use ssl mode, wo need to decrypt received net data(ciphertext) to app data(plaintext).
        decryptData(dstBuf, isHeader);
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
        sslEngine.unwrap(dstBuf, decryptAppData);
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
            if (isSslMode || isSslHandshaking) {
                sslHeaderByteBuffer.clear();
                readLen = readAll(sslHeaderByteBuffer, true);
                if (readLen != SSL_PACKET_HEADER_LEN) {
                    // remote has close this channel
                    LOG.debug("Receive ssl packet header failed, remote may close the channel.");
                    return null;
                }
                // when handshaking and ssl mode, sslengine unwrap need a packet with header.
                result.put(sslHeaderByteBuffer.array());
            } else {
                headerByteBuffer.clear();
                readLen = readAll(headerByteBuffer, true);
                if (readLen != PACKET_HEADER_LEN) {
                    // remote has close this channel
                    LOG.debug("Receive packet header failed, remote may close the channel.");
                    return null;
                }
                if (packetId() != sequenceId) {
                    LOG.warn("receive packet sequence id[" + packetId() + "] want to get[" + sequenceId + "]");
                    throw new IOException("Bad packet sequence.");
                }
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
            readLen = readAll(result, false);
            if (isSslMode) {
                byte[] header = result.array();
                int packetId = header[3] & 0xFF;
                if (packetId != sequenceId) {
                    LOG.warn("receive packet sequence id[" + packetId() + "] want to get[" + sequenceId + "]");
                    throw new IOException("Bad packet sequence.");
                }
                result.position(4);
                result.compact();
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

    protected void realNetSend(ByteBuffer buffer) throws IOException {
        encryptData(buffer);
        long bufLen = buffer.remaining();
        long writeLen = channel.write(buffer);
        if (bufLen != writeLen) {
            throw new IOException("Write mysql packet failed.[write=" + writeLen
                + ", needToWrite=" + bufLen + "]");
        }
        channel.write(buffer);
        isSend = true;
    }

    protected void encryptData(ByteBuffer dstBuf) throws SSLException {
        if (!isSslMode) {
            return;
        }
        encryptNetData.clear();
        // todo: handle BUFFER_OVERFLOW
        sslEngine.wrap(dstBuf, encryptNetData);
        encryptNetData.flip();
        dstBuf.clear();
        dstBuf.put(encryptNetData);
        dstBuf.flip();
    }

    public void flush() throws IOException {
        ByteBuffer sendData = (isSslMode | isSslHandshaking) ? sendSslBuffer : sendBuffer;
        if (null == sendData || sendData.position() == 0) {
            // Nothing to send
            return;
        }
        sendData.flip();
        realNetSend(sendData);
        sendData.clear();
        isSend = true;
    }

    private void writeHeader(int length, boolean isSsl) throws IOException {
        ByteBuffer sendData = isSsl ? sendSslBuffer : sendBuffer;
        if (null == sendData) {
            return;
        }
        long leftLength = sendData.capacity() - sendData.position();
        if (leftLength < 4) {
            flush();
        }

        long newLen = length;
        for (int i = 0; i < 3; ++i) {
            sendData.put((byte) newLen);
            newLen >>= 8;
        }
        sendData.put((byte) sequenceId);
    }

    private void writeBuffer(ByteBuffer buffer, boolean isSsl) throws IOException {
        ByteBuffer sendData = isSsl ? sendSslBuffer : sendBuffer;
        if (null == sendData) {
            return;
        }
        long leftLength = sendData.capacity() - sendData.position();
        // If too long for buffer, send buffered data.
        if (leftLength < buffer.remaining()) {
            // Flush data in buffer.
            flush();
        }
        // Send this buffer if large enough
        if (buffer.remaining() > sendData.capacity()) {
            realNetSend(buffer);
            return;
        }
        // Put it to
        sendData.put(buffer);
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
                writeBuffer(packet, true);
            } else {
                writeHeader(bufLen, isSslMode);
                writeBuffer(packet, isSslMode);
                accSequenceId();
            }
        }
        if (isSslHandshaking) {
            packet.limit(oldLimit);
            writeBuffer(packet, true);
        } else {
            writeHeader(oldLimit - packet.position(), isSslMode);
            packet.limit(oldLimit);
            writeBuffer(packet, isSslMode);
            accSequenceId();
        }
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
}
