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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Proxy protocol handler.
 * The proxy protocol is a simple protocol to pass client connection information to the server.
 * It is used in some load balancers and proxies to pass the client's IP address and port to the server.
 * The protocol is defined in https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt
 * The protocol has two versions: V1 and V2.
 * V1 is a text-based protocol, and V2 is a binary protocol.
 * This class only supports V1.
 * The V1 protocol is a text-based protocol, and the header is "PROXY ".
 * The protocol is defined as:
 * PROXY TCP4[TCP6] <srcip> <dstip> <srcport> <dstport>\r\n
 * or
 * PROXY UNKNOWN xxxx\r\n
 */
public class ProxyProtocolHandler {
    private static final Logger LOG = LogManager.getLogger(ProxyProtocolHandler.class);

    private static final byte[] V1_HEADER = "PROXY ".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] V2_HEADER
            = new byte[] {0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A};

    private static final String UNKNOWN = "UNKNOWN";
    private static final String TCP4 = "TCP4";
    private static final String TCP6 = "TCP6";

    public static class ProxyProtocolResult {
        public String sourceIP = null;
        public int sourcePort = -1;
        public String destIp = null;
        public int destPort = -1;
        public boolean isUnknown = false;

        @Override
        public String toString() {
            return "ProxyProtocolResult{"
                    + "sourceIP='" + sourceIP + '\''
                    + ", sourcePort=" + sourcePort
                    + ", destIp='" + destIp + '\''
                    + ", destPort=" + destPort
                    + ", isUnknown=" + isUnknown
                    + '}';
        }
    }

    public static ProxyProtocolResult handle(BytesChannel channel) throws IOException {
        // First read 1 byte to see if it is V1 or V2
        ByteBuffer buffer = ByteBuffer.allocate(1);
        int readLen = channel.read(buffer);
        if (readLen != 1) {
            throw new IOException("Invalid proxy protocol, expect incoming bytes first");
        }
        buffer.flip();
        byte firstByte = buffer.get();
        if ((char) firstByte == V1_HEADER[0]) {
            return handleV1(channel);
        } else if (firstByte == V2_HEADER[0]) {
            return handleV2(channel);
        } else {
            throw new IOException("Invalid proxy protocol header in first bytes: " + firstByte + ".");
        }
    }

    private static ProxyProtocolResult handleV1(BytesChannel channel) throws IOException {
        ProxyProtocolResult result = new ProxyProtocolResult();

        int byteCount = 1; // already read the first byte, so start with 1
        boolean parsingUnknown = false; // true if "UNKNOWN" is found
        boolean carriageFound = false;  // true if \r is found
        String protocol = null;
        StringBuilder stringBuilder = new StringBuilder();

        // read last 5 bytes of "PROXY "
        ByteBuffer buffer = ByteBuffer.allocate(5);
        int readLen = channel.read(buffer);
        if (readLen != 5) {
            throw new IOException("Invalid proxy protocol v1, expected \"PROXY \"");
        }
        byteCount += readLen;
        StringBuilder debugInfo = new StringBuilder("PROXY ");
        // start reading
        buffer = ByteBuffer.allocate(1);
        channel.read(buffer);
        buffer.flip();
        while (buffer.hasRemaining()) {
            char c = (char) buffer.get();
            debugInfo.append(c);
            if (parsingUnknown) {
                // Found "PROXY UNKNOWN"
                // ignore any other bytes until "\r\n"
                if (c == '\r') {
                    carriageFound = true;
                } else if (c == '\n') {
                    if (!carriageFound) {
                        throw new ProtocolException("Invalid proxy protocol v1. '\\r' is not found before '\\n'",
                                debugInfo.toString());
                    }
                    result.isUnknown = true;
                    return result;
                } else if (carriageFound) {
                    throw new ProtocolException("Invalid proxy protocol v1. "
                            + "'\\r' should follow with '\\n', but see: " + c + ".", debugInfo.toString());
                }
            } else if (carriageFound) {
                if (c == '\n') {
                    // eof, set remote ip
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Finish parsing proxy protocol v1. result: {}", result);
                    }
                    return result;
                } else {
                    throw new ProtocolException("Invalid proxy protocol v1. "
                            + "'\\r' should follow with '\\n', but see: " + c + ".", debugInfo.toString());
                }
            } else {
                switch (c) {
                    case ' ':
                        if (result.sourcePort != -1 || stringBuilder.length() == 0) {
                            throw new ProtocolException("Invalid proxy protocol v1. expecting a '\\r' or a '\\n'",
                                    debugInfo.toString());
                        } else if (protocol == null) {
                            protocol = stringBuilder.toString();
                            stringBuilder.setLength(0);
                            if (protocol.equals(UNKNOWN)) {
                                parsingUnknown = true;
                            } else if (!protocol.equals(TCP4) && !protocol.equals(TCP6)) {
                                throw new ProtocolException("Invalid proxy protocol v1. expecting TCP4/TCP6/UNKNOWN."
                                        + " See: " + protocol + ".", debugInfo.toString());
                            }
                        } else if (result.sourceIP == null) {
                            result.sourceIP = stringBuilder.toString();
                            stringBuilder.setLength(0);
                        } else if (result.destIp == null) {
                            result.destIp = stringBuilder.toString();
                            stringBuilder.setLength(0);
                        } else {
                            result.sourcePort = Integer.parseInt(stringBuilder.toString());
                            stringBuilder.setLength(0);
                        }
                        break;
                    case '\r':
                        if (result.destPort == -1 && result.sourcePort != -1
                                && !carriageFound && stringBuilder.length() > 0) {
                            result.destPort = Integer.parseInt(stringBuilder.toString());
                            stringBuilder.setLength(0);
                            carriageFound = true;
                        } else if (protocol == null) {
                            if (UNKNOWN.equals(stringBuilder.toString())) {
                                parsingUnknown = true;
                                carriageFound = true;
                            }
                        } else {
                            throw new ProtocolException(
                                    "Invalid proxy protocol v1. Already see '\\r' but no valid info",
                                    debugInfo.toString());
                        }
                        break;
                    case '\n':
                        throw new ProtocolException("Invalid proxy protocol v1. '\\r' is not found before '\\n'",
                                debugInfo.toString());
                    default:
                        stringBuilder.append(c);
                }
            }
            byteCount++;
            if (byteCount == 107) {
                throw new ProtocolException("Invalid proxy protocol v1, max length(107) exceeds",
                        debugInfo.toString());
            } else {
                buffer.clear();
                channel.read(buffer);
                buffer.flip();
            }
        }
        throw new ProtocolException("Invalid proxy protocol v1, unexpected end of stream", debugInfo.toString());
    }

    private static ProxyProtocolResult handleV2(BytesChannel channel) throws IOException {
        throw new IOException("proxy protocol v2 is not supported yet");
    }

    public static class ProtocolException extends IOException {
        public ProtocolException(String message, String protocolStr) {
            super(message + ": " + protocolStr);
        }
    }
}
