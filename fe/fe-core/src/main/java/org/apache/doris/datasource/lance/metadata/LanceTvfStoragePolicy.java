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

package org.apache.doris.datasource.lance.metadata;

import org.apache.doris.common.AnalysisException;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.common.net.InetAddresses;
import org.lance.BasePath;
import org.lance.Dataset;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Set;

/** Storage admission for independent Lance TVFs; catalog reads retain their configured storage. */
public final class LanceTvfStoragePolicy {
    // Remote providers registered by the pinned Lance SDK. File providers also have aliases,
    // so checking only for "file" (or a particular StorageProperties type) is insufficient.
    private static final Set<String> LANCE_REMOTE_SCHEMES = ImmutableSet.of(
            "s3", "s3+ddb", "az", "abfss", "gs", "goosefs", "oss", "cos", "hf", "tos");

    private LanceTvfStoragePolicy() {
    }

    /** Inspect manifest registrations before BASIC metadata can open any referenced fragments. */
    public static void checkRemoteBases(Dataset dataset) throws AnalysisException {
        for (BasePath base : dataset.getBasePaths()) {
            if (!isRemoteUri(base.getPath())) {
                throw new AnalysisException("Remote Lance TVF requires remote manifest storage bases");
            }
        }
    }

    public static boolean isRemoteUri(String path) {
        try {
            if (path == null) {
                return false;
            }
            int colon = path.indexOf(':');
            if (colon < 0 || !LANCE_REMOTE_SCHEMES.contains(path.substring(0, colon).toLowerCase(Locale.ROOT))) {
                return false;
            }
            String scheme = path.substring(0, colon);
            if (!path.startsWith("//", colon + 1)) {
                // GooseFS can resolve its remote master from storage options or configuration.
                return "goosefs".equalsIgnoreCase(scheme) && path.startsWith("/", colon + 1);
            }
            int authorityEnd = path.length();
            for (char separator : new char[] {'/', '?', '#'}) {
                int end = path.indexOf(separator, colon + 3);
                if (end >= 0) {
                    authorityEnd = Math.min(authorityEnd, end);
                }
            }
            // Validate only scheme/authority against SDK-local fallback. Rust's URL parser
            // accepts and encodes raw spaces in paths; preserve that spelling for the SDK.
            URI uri = new URI(path.substring(0, authorityEnd) + "/");
            String authority = uri.getRawAuthority();
            if (authority == null || authority.isEmpty()) {
                return "goosefs".equalsIgnoreCase(scheme);
            }
            // Lance falls back to a local path if Rust's URL parser rejects the URI. Java's URI
            // accepts registry authorities with invalid ports, which Rust rejects. Validate the
            // host/port separately without disallowing legitimate bucket names with underscores.
            String hostPort = authority.substring(authority.lastIndexOf('@') + 1);
            HostAndPort parsed = HostAndPort.fromString(hostPort).requireBracketsForIPv6();
            if (parsed.hasPort()) {
                // Guava parses "-0" as zero, but Rust rejects signs and falls back to local.
                String port = hostPort.substring(hostPort.lastIndexOf(':') + 1);
                for (int i = 0; i < port.length(); i++) {
                    if (port.charAt(i) < '0' || port.charAt(i) > '9') {
                        return false;
                    }
                }
            }
            String host = parsed.getHost();
            // Rust rejects zones and IPv4 tails with leading zeros, such as ::ffff:127.0.0.01.
            return !host.isEmpty() && (!host.contains(":")
                    || (!host.contains("%") && InetAddresses.isInetAddress(host)));
        } catch (URISyntaxException | IllegalArgumentException e) {
            return false;
        }
    }

}
