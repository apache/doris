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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ProxyProtocolHandler {
    private static final Logger LOG = LogManager.getLogger(ProxyProtocolHandler.class);

    private static final byte[] V1_HEADER = "PROXY ".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] V2_HEADER = new byte[] {0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A};

    private static final String UNKNOWN = "UNKNOWN";
    private static final String TCP4 = "TCP4";
    private static final String TCP6 = "TCP6";

    public static void handle(ConnectContext context) throws IOException {
        MysqlChannel channel = context.getMysqlChannel();
        // First read 1 byte to see if it is V1 or V2
        ByteBuffer buffer = ByteBuffer.allocate(1);
        int readLen = channel.read(buffer);
        if (readLen != 1) {
            throw new IOException("Invalid proxy protocol, expect incoming bytes first");
        }
        buffer.flip();
        byte firstByte = buffer.get();
        if (firstByte == V2_HEADER[0]) {  // Could be Proxy Protocol V2
            handleV2(context);
        } else if ((char) firstByte == V1_HEADER[0]){ // Could be Proxy Protocol V1
            handleV1(context);
        } else {
            throw new IOException("Invalid proxy protocol header in first bytes: " + firstByte + ".");
        }
    }

    private static void handleV1(ConnectContext context) throws IOException {
        MysqlChannel channel = context.getMysqlChannel();
        int byteCount = 1; // already read the first byte, so start with 1
        boolean parsingUnknown = false; // true if "UNKNOWN" is found
        boolean carriageFound = false;  // true if \r is found
        InetAddress sourceAddress = null;
        InetAddress destAddress = null;
        int sourcePort = -1;
        int destPort = -1;
        String protocol = null;
        StringBuilder stringBuilder = new StringBuilder();

        // read last 5 bytes of "PROXY "
        ByteBuffer buffer = ByteBuffer.allocate(5);
        int readLen = channel.read(buffer);
        if (readLen != 5) {
            throw new IOException("Invalid proxy protocol v1, expected \"PROXY \"");
        }
        byteCount += readLen;

        // start reading
        buffer = ByteBuffer.allocate(1);
        channel.read(buffer);
        buffer.flip();
        while (buffer.hasRemaining()) {
            char c = (char) buffer.get();
            if (parsingUnknown) {
                // Found "PROXY UNKNOWN"
                // ignore any other bytes until "\r\n"
                if (c == '\r') {
                    carriageFound = true;
                } else if (c == '\n') {
                    if (!carriageFound) {
                        throw new IOException("Invalid proxy protocol v1. '\\r' is not found before '\\n'");
                    }
                    break;
                } else if (carriageFound) {
                    throw new IOException("Invalid proxy protocol v1. "
                            + "'\\r' should follow with '\\n', but see: " + c + ".");
                }
            } else if (carriageFound) {
                if (c == '\n') {
                    // eof, set remote ip
                    context.setRemoteIP(sourceAddress.getHostAddress());
                    if (LOG.isDebugEnabled()) {
                        SocketAddress s = new InetSocketAddress(sourceAddress, sourcePort);
                        SocketAddress d = new InetSocketAddress(destAddress, destPort);
                        LOG.debug("Finish parsing proxy protocol v1. source: {}, dest: {}", s, d);
                    }
                    break;
                } else {
                    throw new IOException("Invalid proxy protocol v1. "
                            + "'\\r' should follow with '\\n', but see: " + c + ".");
                }
            } else {
                switch (c) {
                    case ' ':
                        if (sourcePort != -1 || stringBuilder.length() == 0) {
                            throw new IOException("Invalid proxy protocol v1. expecting a '\\r' or a '\\n'");
                        } else if (protocol == null) {
                            protocol = stringBuilder.toString();
                            stringBuilder.setLength(0);
                            if (protocol.equals(UNKNOWN)) {
                                parsingUnknown = true;
                            } else if (!protocol.equals(TCP4) && !protocol.equals(TCP6)) {
                                throw new IOException("Invalid proxy protocol v1. expecting TCP4/TCP6/UNKNOWN."
                                        + " See: " + protocol + ".");
                            }
                        } else if (sourceAddress == null) {
                            sourceAddress = parseAddress(stringBuilder.toString(), protocol);
                            stringBuilder.setLength(0);
                        } else if (destAddress == null) {
                            destAddress = parseAddress(stringBuilder.toString(), protocol);
                            stringBuilder.setLength(0);
                        } else {
                            sourcePort = Integer.parseInt(stringBuilder.toString());
                            stringBuilder.setLength(0);
                        }
                        break;
                    case '\r':
                        if (destPort == -1 && sourcePort != -1 && !carriageFound && stringBuilder.length() > 0) {
                            destPort = Integer.parseInt(stringBuilder.toString());
                            stringBuilder.setLength(0);
                            carriageFound = true;
                        } else if (protocol == null) {
                            if (UNKNOWN.equals(stringBuilder.toString())) {
                                parsingUnknown = true;
                                carriageFound = true;
                            }
                        } else {
                            throw new IOException("Invalid proxy protocol v1. Already see '\\r' but no valid info");
                        }
                        break;
                    case '\n':
                        throw new IOException("Invalid proxy protocol v1. '\\r' is not found before '\\n'");
                    default:
                        stringBuilder.append(c);
                }
            }
            byteCount++;
            if (byteCount == 107) {
                throw new IOException("Invalid proxy protocol v1, max length(107) exceeds");
            } else {
                buffer.clear();
                channel.readAll(buffer, false);
                buffer.flip();
            }
        }
    }

    private static void handleV2(ConnectContext context) {
        // TODO
    }

    private static InetAddress parseAddress(String addressString, String protocol) throws IOException {
        if (protocol.equals(TCP4)) {
            return parseIpv4Address(addressString);
        } else {
            return parseIpv6Address(addressString);
        }
    }

    private static InetAddress parseIpv4Address(String ipStr) throws IOException {
        String[] parts = ipStr.split("\\.");
        if (parts.length != 4) {
            throw new IOException("Invalid ipv4 format: " + ipStr);
        }
        byte[] data = new byte[4];
        for (int i = 0; i < 4; ++i) {
            String part = parts[i];
            if (part.length() == 0 || (part.charAt(0) == '0' && part.length() > 1)) {
                // leading zeros are not allowed
                throw new IOException("Invalid ipv4 format: " + ipStr);
            }
            data[i] = (byte) Integer.parseInt(part);
        }
        return InetAddress.getByAddress(data);
    }

    public static InetAddress parseIpv6Address(String ipStr) throws IllegalArgumentException, IOException {
        return InetAddress.getByAddress(parseIpv6AddressToBytes(ipStr));
    }

    public static byte[] parseIpv6AddressToBytes(String ipStr) throws IllegalArgumentException, IOException {
        boolean startsWithColon = ipStr.startsWith(":");
        if (startsWithColon && !ipStr.startsWith("::")) {
            throw new IOException("invalid ipv6");
        }
        String[] parts = splitIPv6(ipStr);
        byte[] data = new byte[16];
        int partOffset = 0;
        boolean seenEmpty = false;
        if (parts.length > 8) {
            throw new IOException("invalid ipv6");
        }
        for (int i = 0; i < parts.length; ++i) {
            String part = parts[i];
            if (part.length() > 4) {
                throw new IOException("invalid ipv6");
            } else if (part.isEmpty()) {
                if (seenEmpty) {
                    throw new IOException("invalid ipv6");
                }
                seenEmpty = true;
                int off = 8 - parts.length;
                if (off < 0) {
                    throw new IOException("invalid ipv6");
                }
                partOffset = off * 2;
            } else {
                int num = Integer.parseInt(part, 16);
                data[i * 2 + partOffset] = (byte) (num >> 8);
                data[i * 2 + partOffset + 1] = (byte) (num);
            }
        }
        if ((parts.length < 8 && !ipStr.endsWith("::")) && !seenEmpty) {
            //address was too small
            throw new IOException("invalid ipv6");
        }
        return data;
    }

    private static String[] splitIPv6(String ipStr) {
        final List<String> list = new ArrayList<>();
        final int size = ipStr.length();
        char previous = 0;
        final StringBuilder bits = new StringBuilder(8);
        for(int i = 0; i<size;i++) {
            final char current = ipStr.charAt(i);
            if(current != ':'){
                bits.append(current);
            }

            if(previous == current && current == ':') {
                list.add("");
            } else if (((current == ':') || (i == size - 1)) && bits.length() > 0) {
                list.add(bits.toString());
                bits.setLength(0);
            }
            previous = current;
        }
        return list.toArray(new String[list.size()]);
    }
}
