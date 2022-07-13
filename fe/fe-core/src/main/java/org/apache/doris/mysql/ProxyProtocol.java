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
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ProxyProtocol {
    // max len of proxy protocol v1
    private static final int MAX_HEADER_LENGTH = 107;
    // size of proxy protocol v2 header
    private static final int PROXY_V2_HEADER_LEN = 16;

    // logger for this class
    protected static final Logger LOG = LogManager.getLogger(ProxyProtocol.class);
    // signature for proxy protocol v1
    private static final byte[] SIG_V1 = "PROXY ".getBytes(StandardCharsets.US_ASCII);
    private static final String UNKNOWN = "UNKNOWN";
    private static final String TCP4 = "TCP4";
    private static final String TCP6 = "TCP6";

    // signature for proxy protocol v2
    private static final byte[] SIG_V2 = new byte[]{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A};

    static class ConnectInfo{
        public InetAddress sourceAddress;
        public int sourcePort;
        public InetAddress destAddress;
        public int destPort;

        public ConnectInfo() {
        }
    };

    public static void processProtocolHeaderV1(ByteBuffer proxyPayload, ConnectInfo connInfo) throws IOException {
        int byteCount = 0;
        String protocol = null;
        StringBuilder stringBuilder = new StringBuilder();
        boolean carriageReturnSeen = false;
        boolean parsingUnknown = false;

        int sourcePort = -1;
        InetAddress sourceAddress = null;
        InetAddress destAddress = null;
        int destPort = -1;

        proxyPayload.flip();
        while (proxyPayload.hasRemaining()) {
            char c = (char) proxyPayload.get();
            if (byteCount < SIG_V1.length) {
                //first we verify that we have the correct protocol
                if (c != SIG_V1[byteCount]) {
                    throw new IOException("Error msg header.");
                }
            } else {
                if (parsingUnknown) {
                    //we are parsing the UNKNOWN protocol
                    //we just ignore everything till \r\n
                    if (c == '\r') {
                        carriageReturnSeen = true;
                    } else if (c == '\n') {
                        if (!carriageReturnSeen) {
                            throw new IOException("Error msg header.");
                        }
                        return;
                    } else if (carriageReturnSeen) {
                        throw new IOException("Error msg header.");
                    }
                } else if (carriageReturnSeen) {
                    if (c == '\n'){
                        return;
                    } else {
                        throw new IOException("Error msg header.");
                    }
                } else switch (c) {
                    case ' ':
                        //we have a space
                        if (sourcePort != -1 || stringBuilder.length() == 0) {
                            //header was invalid, either we are expecting a \r or a \n, or the previous character was a space
                            throw new IOException("Error msg header.");
                        } else if (protocol == null) { // protocol
                            protocol = stringBuilder.toString();
                            stringBuilder.setLength(0);
                            if (protocol.equals(UNKNOWN)) {
                                parsingUnknown = true;
                            } else if (!protocol.equals(TCP4) && !protocol.equals(TCP6)) {
                                throw new IOException("Error msg header.");
                            }
                        } else if (sourceAddress == null) { // source ip
                            sourceAddress = InetAddress.getByName(stringBuilder.toString());
                            connInfo.sourceAddress = sourceAddress;
                            stringBuilder.setLength(0);
                        } else if (destAddress == null) { // dest ip
                            destAddress = InetAddress.getByName(stringBuilder.toString());
                            connInfo.destAddress = destAddress;
                            stringBuilder.setLength(0);
                        } else { // source port
                            sourcePort = Integer.parseInt(stringBuilder.toString());
                            connInfo.sourcePort = sourcePort;
                            stringBuilder.setLength(0);
                        }
                        break;
                    case '\r':
                        // dest port
                        if (destPort == -1 && sourcePort != -1 && !carriageReturnSeen && stringBuilder.length() > 0) {
                            destPort = Integer.parseInt(stringBuilder.toString());
                            connInfo.destPort = destPort;
                            stringBuilder.setLength(0);
                            carriageReturnSeen = true;
                        } else if (protocol == null) {
                            if (UNKNOWN.equals(stringBuilder.toString())) {
                                parsingUnknown = true;
                                carriageReturnSeen = true;
                            }
                        } else {
                            throw new IOException("Error msg header.");
                        }
                        break;
                    case '\n':
                        throw new IOException("Error msg header.");
                    default:
                        stringBuilder.append(c);
                }
            }

            byteCount++;
            if (byteCount == MAX_HEADER_LENGTH) {
                throw new IOException("Error msg header.");
            }
        }
    }

    private static void parseProxyProtocolHeaderV1(MysqlChannel channel, ByteBuffer buffer) throws IOException {
        ByteBuffer proxyPayload = ByteBuffer.allocate(MAX_HEADER_LENGTH+1);
        proxyPayload.put(buffer);

        while (proxyPayload.remaining() != 0) {
            ByteBuffer readbuffer = ByteBuffer.allocate(1);
            channel.readAll(readbuffer);
            if(readbuffer.hasRemaining()) {
                throw new IOException("Error proxy protocol v1 header");
            }

            readbuffer.flip();
            byte val = readbuffer.get();
            proxyPayload.put(val);
            // break when get the end of proxy protocol v1 header
            if (val == 0x0a) {
                break;
            }
        }

        try{
            ConnectInfo connInfo = new ConnectInfo();
            processProtocolHeaderV1(proxyPayload, connInfo);
            // set to remote ip and host/port string
            if(connInfo.sourceAddress != null) {
                channel.setRemoteHostPortString(connInfo.sourceAddress.getHostAddress() + ":" + String.valueOf(connInfo.sourcePort));
                channel.setRemoteIp(connInfo.sourceAddress.getHostAddress());
            }
        } catch (IOException e){
            throw new IOException("Error proxy protocol header");
        }
    }

    private static void parseProxyProtocolHeaderV2(MysqlChannel channel, ByteBuffer buffer) throws IOException {
        ByteBuffer readbuffer = ByteBuffer.allocate(PROXY_V2_HEADER_LEN);
        readbuffer.put(buffer);

        channel.readAll(readbuffer);
        if(readbuffer.hasRemaining()) {
            throw new IOException("Error msg header");
        }

        int byteCount = 0;

        readbuffer.flip();
        while (byteCount < SIG_V2.length) {
            byte c = readbuffer.get();

            //first we verify that we have the correct protocol
            if (c != SIG_V2[byteCount]) {
                throw new IOException("Error msg header.");
            }
            byteCount++;
        }

        byte ver_cmd = readbuffer.get();
        byte fam = readbuffer.get();
        int len = (readbuffer.getShort() & 0xffff);

        if ((ver_cmd & 0xF0) != 0x20) {  // expect version 2
            throw new IOException("Error msg header.");
        }

        // read payload
        ByteBuffer payload = ByteBuffer.allocate(len);
        channel.readAll(payload);
        if(payload.hasRemaining()) {
            throw new IOException("Error proxy protocol v2 payload");
        }

        InetAddress sourceAddress = null;
        int sourcePort = -1;
        InetAddress destAddress = null;
        int destPort = -1;

        payload.flip();
        switch (ver_cmd & 0x0F) {
            case 0x01:  // PROXY command
                switch (fam) {
                    case 0x11: { // TCP over IPv4
                        if (len < 12) {
                            throw new IOException("Error msg header.");
                        }

                        byte[] sourceAddressBytes = new byte[4];
                        payload.get(sourceAddressBytes);
                        sourceAddress = InetAddress.getByAddress(sourceAddressBytes);

                        byte[] dstAddressBytes = new byte[4];
                        payload.get(dstAddressBytes);
                        destAddress = InetAddress.getByAddress(dstAddressBytes);

                        sourcePort = payload.getShort() & 0xffff;
                        destPort = payload.getShort() & 0xffff;
                        break;
                    }
                    case 0x21: { // TCP over IPv6
                        if (len < 36) {
                            throw new IOException("Error msg header.");
                        }

                        byte[] sourceAddressBytes = new byte[16];
                        payload.get(sourceAddressBytes);
                        sourceAddress = InetAddress.getByAddress(sourceAddressBytes);

                        byte[] dstAddressBytes = new byte[16];
                        payload.get(dstAddressBytes);
                        destAddress = InetAddress.getByAddress(dstAddressBytes);

                        sourcePort = payload.getShort() & 0xffff;
                        destPort = payload.getShort() & 0xffff;
                        break;
                    }
                    default: // AF_UNIX
                        throw new IOException("AF_UNIX sockets not supported.");
                }
                break;
            case 0x00: // LOCAL command
                // log warning msg once hit here
                LOG.warn("LOCAL command for proxy protocol v2");
                return;
            default:
                throw new IOException("Error msg header.");
        }

        // set to remote ip and host:port string
        channel.setRemoteHostPortString(sourceAddress.getHostAddress()+":"+String.valueOf(sourcePort));
        channel.setRemoteIp(sourceAddress.getHostAddress());
    }

    public static void parseProxyProtocol(MysqlChannel channel, ByteBuffer headbuf) throws IOException {
        // check for proxy protocol
        headbuf.flip();
        ByteBuffer proxyHeaderV1 = ByteBuffer.wrap(SIG_V1, 0, headbuf.limit());
        ByteBuffer proxyHeaderV2 = ByteBuffer.wrap(SIG_V2, 0, headbuf.limit());
        if (proxyHeaderV1.compareTo(headbuf) == 0) {
            // match proxy protocol v1
            parseProxyProtocolHeaderV1(channel, headbuf);
        } else if (proxyHeaderV2.compareTo(headbuf) == 0) {
            // match proxy protocol v2
            parseProxyProtocolHeaderV2(channel, headbuf);
        } else {
            throw new IOException("unsupported proxy protocol header.");
        }
    }
}
