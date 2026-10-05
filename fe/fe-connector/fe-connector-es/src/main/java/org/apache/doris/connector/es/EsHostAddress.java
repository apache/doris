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

package org.apache.doris.connector.es;

final class EsHostAddress {

    private EsHostAddress() {
    }

    static ParsedAddress parse(String address, int defaultPort) {
        String normalized = normalize(address);
        if (normalized.startsWith("[")) {
            int bracketEnd = normalized.indexOf(']');
            if (bracketEnd < 0) {
                throw new IllegalArgumentException("Invalid bracketed ES address: " + address);
            }
            String host = normalized.substring(1, bracketEnd);
            if (bracketEnd + 1 >= normalized.length()) {
                return new ParsedAddress(host, defaultPort);
            }
            if (normalized.charAt(bracketEnd + 1) != ':') {
                throw new IllegalArgumentException("Invalid bracketed ES address: " + address);
            }
            return new ParsedAddress(host, Integer.parseInt(normalized.substring(bracketEnd + 2)));
        }

        int lastColon = normalized.lastIndexOf(':');
        if (lastColon < 0) {
            return new ParsedAddress(normalized, defaultPort);
        }

        String portPart = normalized.substring(lastColon + 1);
        if (!portPart.isEmpty() && portPart.chars().allMatch(Character::isDigit)) {
            return new ParsedAddress(normalized.substring(0, lastColon), Integer.parseInt(portPart));
        }
        return new ParsedAddress(normalized, defaultPort);
    }

    static String extractHostname(String address) {
        return parse(address, -1).getHost();
    }

    static String extractSchemePrefix(String address) {
        int schemeEnd = address.indexOf("://");
        return schemeEnd >= 0 ? address.substring(0, schemeEnd + 3) : "";
    }

    static String formatHostPort(String host, int port) {
        String prefix = "";
        String bareHost = host;
        String schemePrefix = extractSchemePrefix(host);
        if (!schemePrefix.isEmpty()) {
            prefix = schemePrefix;
            bareHost = host.substring(prefix.length());
        }
        if (bareHost.contains(":") && !bareHost.startsWith("[")) {
            bareHost = "[" + bareHost + "]";
        }
        return prefix + bareHost + ":" + port;
    }

    private static String normalize(String address) {
        int schemeEnd = address.indexOf("://");
        String normalized = schemeEnd >= 0 ? address.substring(schemeEnd + 3) : address;
        int pathIdx = normalized.indexOf('/');
        if (pathIdx >= 0) {
            normalized = normalized.substring(0, pathIdx);
        }
        return normalized;
    }

    static final class ParsedAddress {
        private final String host;
        private final int port;

        private ParsedAddress(String host, int port) {
            this.host = host;
            this.port = port;
        }

        String getHost() {
            return host;
        }

        int getPort() {
            return port;
        }
    }
}
