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

package org.apache.doris.connector.spi;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

/**
 * The mandatory, non-configurable {@code driver_url} security rule, shared by every connector that
 * loads a JDBC driver jar into the FE JVM.
 *
 * <p>Three catalog types reach the same {@code URLClassLoader} + {@code Class.forName(name, true, loader)}
 * sink from a user-supplied catalog property, so they must share one rule rather than each re-deriving it:
 * the {@code jdbc} catalog ({@code driver_url}), the Iceberg JDBC catalog
 * ({@code iceberg.jdbc.driver_url}) and the Paimon JDBC catalog
 * ({@code paimon.jdbc.driver_url} / {@code jdbc.driver_url}). This class is that single source of truth;
 * it lives in the SPI module because it is the only module all three connectors depend on.
 *
 * <p>The rule cannot be turned off:
 * <ul>
 *   <li>any {@code ..} path-traversal segment is rejected, for {@code file://} and {@code http(s)} alike,
 *       checked on the percent-decoded path so {@code %2e%2e} cannot slip past;</li>
 *   <li>a scheme-less driver_url must be a bare jar file name matching {@code [A-Za-z0-9._-]+.jar}
 *       (no directories, no special characters), which is then resolved under the connector's drivers
 *       directory.</li>
 * </ul>
 * Whether a remote/absolute URL is allowed <em>at all</em> remains governed by the fe.conf-only
 * {@code jdbc_driver_secure_path} / {@code jdbc_driver_url_white_list} configs, applied separately through
 * {@code ConnectorValidationContext.validateAndResolveDriverPath}; this rule only forbids traversal and
 * enforces the bare-name charset.
 *
 * <p><b>Where callers must invoke it.</b> From the provider's {@code validateProperties} — the engine's
 * {@code checkProperties()} hook, which runs on the user-facing CREATE <em>and</em> ALTER CATALOG paths
 * (both guarded by {@code !isReplay}) — so a malicious driver_url cannot be introduced by either. It must
 * never be run during metadata/edit-log replay or at query time, so existing catalogs are unaffected and
 * FE startup / follower replay can never be blocked by it.
 *
 * <p>Throws {@link IllegalArgumentException} so the engine wraps it into a {@code DdlException}
 * (and, on ALTER, triggers the property rollback).
 */
public final class JdbcDriverUrlSecurity {

    // A scheme-less driver_url must be a plain jar file name: letters, digits, dot, underscore, hyphen.
    // This intentionally forbids any path separator, so it can never escape the drivers directory.
    private static final Pattern SAFE_DRIVER_FILE_NAME = Pattern.compile("^[A-Za-z0-9._-]+\\.jar$");

    private JdbcDriverUrlSecurity() {
    }

    /**
     * Applies the rule to a raw, alias-resolved {@code driver_url}. A null/empty value means "use the
     * engine-provided driver" and is accepted; every other value must satisfy the rule above.
     */
    public static void check(String driverUrl) {
        if (driverUrl == null || driverUrl.isEmpty()) {
            return;
        }
        // Check traversal on the decoded path so percent-encoded segments (e.g. %2e%2e) — which the
        // driver-loading consumers decode — cannot slip a ".." past this rule.
        String pathToCheck = driverUrl;
        if (driverUrl.contains("://")) {
            try {
                String decoded = new URI(driverUrl).getPath();
                if (decoded != null) {
                    pathToCheck = decoded;
                }
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid driver_url: " + driverUrl);
            }
        }
        String probe = pathToCheck.replace('\\', '/');
        for (String segment : probe.split("/")) {
            if ("..".equals(segment)) {
                throw new IllegalArgumentException(
                        "Invalid driver_url: path traversal ('..') is not allowed: " + driverUrl);
            }
        }
        if (!driverUrl.contains("://")) {
            if (!SAFE_DRIVER_FILE_NAME.matcher(driverUrl).matches()) {
                throw new IllegalArgumentException(
                        "Invalid driver_url: a driver file name must match [A-Za-z0-9._-]+.jar (got: "
                                + driverUrl + ")");
            }
        }
    }
}
