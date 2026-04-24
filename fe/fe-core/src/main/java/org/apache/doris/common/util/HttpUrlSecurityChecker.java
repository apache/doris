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

package org.apache.doris.common.util;

import org.apache.doris.common.CIDR;
import org.apache.doris.common.UserException;

import org.apache.commons.validator.routines.InetAddressValidator;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Locale;

public final class HttpUrlSecurityChecker {
    private HttpUrlSecurityChecker() {
    }

    public static void validate(String url, String[] allowlist) throws UserException {
        URI uri = parse(url);
        String scheme = uri.getScheme();
        if (scheme == null) {
            throw new UserException("HTTP TVF URL scheme is empty: " + url);
        }
        scheme = scheme.toLowerCase(Locale.ROOT);
        if (!"http".equals(scheme) && !"https".equals(scheme)) {
            throw new UserException("HTTP TVF only supports http and https URLs: " + url);
        }
        if (uri.getUserInfo() != null) {
            throw new UserException("HTTP TVF URL must not include user info: " + url);
        }

        String host = normalizeHost(uri.getHost());
        if (host.isEmpty()) {
            throw new UserException("HTTP TVF URL host is empty: " + url);
        }
        if (isHostAllowed(host, allowlist)) {
            return;
        }

        InetAddress literalAddress = parseLiteralAddress(host);
        if (literalAddress != null) {
            validateAddress(host, literalAddress, allowlist);
            return;
        }

        try {
            InetAddress[] addresses = InetAddress.getAllByName(host);
            for (InetAddress address : addresses) {
                validateAddress(host, address, allowlist);
            }
        } catch (UnknownHostException e) {
            throw new UserException("Failed to resolve HTTP TVF URL host: " + host, e);
        }
    }

    private static URI parse(String url) throws UserException {
        if (url == null || url.trim().isEmpty()) {
            throw new UserException("HTTP TVF URL is empty");
        }
        try {
            return new URI(url);
        } catch (URISyntaxException e) {
            throw new UserException("Invalid HTTP TVF URL: " + url, e);
        }
    }

    private static void validateAddress(String host, InetAddress address, String[] allowlist) throws UserException {
        if (isAddressAllowed(address, allowlist)) {
            return;
        }
        if (isUnsafeAddress(address)) {
            throw new UserException("HTTP TVF URL resolves to unsafe address: " + host
                    + " -> " + address.getHostAddress());
        }
    }

    private static boolean isHostAllowed(String host, String[] allowlist) {
        if (allowlist == null) {
            return false;
        }
        for (String entry : allowlist) {
            if (entry == null || entry.trim().isEmpty() || entry.contains("/")) {
                continue;
            }
            String normalizedEntry = normalizeHost(entry.trim());
            if (host.equals(normalizedEntry)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isAddressAllowed(InetAddress address, String[] allowlist) throws UserException {
        if (allowlist == null) {
            return false;
        }
        for (String entry : allowlist) {
            if (entry == null || entry.trim().isEmpty()) {
                continue;
            }
            String trimmedEntry = normalizeHost(entry.trim());
            if (trimmedEntry.contains("/")) {
                if (cidrContains(trimmedEntry, address)) {
                    return true;
                }
                continue;
            }
            InetAddress allowedAddress = parseLiteralAddress(trimmedEntry);
            if (allowedAddress != null && Arrays.equals(address.getAddress(), allowedAddress.getAddress())) {
                return true;
            }
        }
        return false;
    }

    private static boolean cidrContains(String cidr, InetAddress address) throws UserException {
        try {
            return new CIDR(cidr).contains(address.getHostAddress());
        } catch (IllegalArgumentException e) {
            throw new UserException("Invalid HTTP TVF private endpoint allowlist entry: " + cidr, e);
        }
    }

    private static InetAddress parseLiteralAddress(String host) throws UserException {
        if (!InetAddressValidator.getInstance().isValid(host)) {
            return null;
        }
        try {
            return InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            throw new UserException("Invalid HTTP TVF IP address: " + host, e);
        }
    }

    private static String normalizeHost(String host) {
        if (host == null) {
            return "";
        }
        String normalized = host.trim().toLowerCase(Locale.ROOT);
        if (normalized.startsWith("[") && normalized.endsWith("]")) {
            normalized = normalized.substring(1, normalized.length() - 1);
        }
        if (normalized.endsWith(".")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private static boolean isUnsafeAddress(InetAddress address) {
        if (address.isAnyLocalAddress() || address.isLoopbackAddress() || address.isLinkLocalAddress()
                || address.isSiteLocalAddress() || address.isMulticastAddress()) {
            return true;
        }
        byte[] bytes = address.getAddress();
        if (bytes.length == 4) {
            return isUnsafeIpv4(bytes, 0);
        }
        if (bytes.length == 16) {
            if (isIpv4MappedIpv6(bytes)) {
                return isUnsafeIpv4(bytes, 12);
            }
            if (isIpv4CompatibleIpv6(bytes)) {
                return true;
            }
            return isUnsafeIpv6(bytes);
        }
        return true;
    }

    private static boolean isUnsafeIpv4(byte[] bytes, int offset) {
        int first = bytes[offset] & 0xff;
        int second = bytes[offset + 1] & 0xff;
        return first == 0
                || first == 10
                || first == 127
                || (first == 100 && second >= 64 && second <= 127)
                || (first == 169 && second == 254)
                || (first == 172 && second >= 16 && second <= 31)
                || (first == 192 && second == 0
                    && ((bytes[offset + 2] & 0xff) == 0 || (bytes[offset + 2] & 0xff) == 2))
                || (first == 192 && second == 88 && (bytes[offset + 2] & 0xff) == 99)
                || (first == 192 && second == 168)
                || (first == 198 && (second == 18 || second == 19))
                || (first == 198 && second == 51 && (bytes[offset + 2] & 0xff) == 100)
                || (first == 203 && second == 0 && (bytes[offset + 2] & 0xff) == 113)
                || first >= 224;
    }

    private static boolean isIpv4MappedIpv6(byte[] bytes) {
        for (int i = 0; i < 10; i++) {
            if (bytes[i] != 0) {
                return false;
            }
        }
        return bytes[10] == (byte) 0xff && bytes[11] == (byte) 0xff;
    }

    private static boolean isIpv4CompatibleIpv6(byte[] bytes) {
        for (int i = 0; i < 12; i++) {
            if (bytes[i] != 0) {
                return false;
            }
        }
        return true;
    }

    private static boolean isUnsafeIpv6(byte[] bytes) {
        int first = bytes[0] & 0xff;
        int second = bytes[1] & 0xff;
        return (first == 0x20 && second == 0x01 && (bytes[2] & 0xff) == 0x0d
                    && (bytes[3] & 0xff) == 0xb8)
                || (first & 0xfe) == 0xfc
                || (first == 0xfe && (second & 0xc0) == 0x80)
                || first == 0xff;
    }
}
