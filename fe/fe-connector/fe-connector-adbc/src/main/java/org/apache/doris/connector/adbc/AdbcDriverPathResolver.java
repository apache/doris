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

package org.apache.doris.connector.adbc;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Turns the {@code driver_url} property into the absolute path of a local ADBC driver shared library.
 *
 * <p><b>Why local only, unlike the JDBC catalog's {@code driver_url}.</b> ADBC partition descriptors are
 * driver-private opaque bytes with no interoperability guarantee across driver implementations -- two
 * official implementations of the SAME protocol already serialize incompatible protobuf messages and
 * mis-parse each other silently rather than erroring. So FE and every BE must load the identical driver
 * file, and a URL each node downloads for itself cannot promise that. Rejecting remote schemes outright
 * keeps that failure -- which surfaces as an unreadable partition, far from its cause -- unreachable.
 *
 * <p>This does NOT reuse {@code ConnectorValidationContext#validateAndResolveDriverPath}: that one resolves
 * against {@code jdbc_drivers_dir} and enforces a {@code .jar} grammar.
 */
public final class AdbcDriverPathResolver {

    /**
     * A scheme-less driver_url must be a plain shared-library file name: letters, digits, dot, underscore,
     * hyphen, ending in {@code .so} or a versioned {@code .so.N[.N...]}. Forbidding every path separator is
     * what makes it impossible to escape the configured drivers directory.
     */
    private static final Pattern SAFE_DRIVER_FILE_NAME =
            Pattern.compile("^[A-Za-z0-9._-]+\\.so(\\.[0-9]+)*$");

    private AdbcDriverPathResolver() {
    }

    /**
     * Resolves and authorizes {@code driverUrl}. Does not touch the filesystem -- see
     * {@link #checkExists(Path, String)} for that half, which is deliberately separate so the grammar and
     * the allow-list can be tested without laying down files.
     *
     * @param driverUrl the raw {@code driver_url} property value
     * @param driversDir directory a bare file name resolves under (adbc.conf's {@code drivers_dir})
     * @param securePath semicolon-separated allowed directories, or {@code *} / blank to allow all
     * @throws IllegalArgumentException on any rejected form, naming what is wrong
     */
    public static Path resolve(String driverUrl, String driversDir, String securePath) {
        if (driverUrl == null || driverUrl.trim().isEmpty()) {
            throw new IllegalArgumentException("Required property '"
                    + AdbcCatalogProperties.DRIVER_URL + "' is missing for an adbc catalog");
        }
        String raw = driverUrl.trim();
        Path candidate = toAbsolutePath(raw, driversDir);
        rejectTraversal(raw, candidate);
        checkSecurePath(raw, candidate, securePath);
        return candidate;
    }

    private static Path toAbsolutePath(String raw, String driversDir) {
        int schemeEnd = raw.indexOf("://");
        if (schemeEnd > 0) {
            String scheme = raw.substring(0, schemeEnd);
            if (!"file".equalsIgnoreCase(scheme)) {
                throw new IllegalArgumentException("Invalid '" + AdbcCatalogProperties.DRIVER_URL
                        + "': scheme '" + scheme + "' is not supported, only a local file is."
                        + " An ADBC driver is not downloaded per node: FE and every BE must load the"
                        + " identical driver library, because partition descriptors are driver-private"
                        + " bytes with no interoperability across driver builds."
                        + " Place the file on FE and on all BEs and reference it by path or by bare name."
                        + " (got: " + raw + ")");
            }
            URI uri;
            try {
                uri = new URI(raw);
            } catch (URISyntaxException e) {
                // Fail closed: an unparsable URL must never be accepted, or the checks below would be
                // validating a different string than the loader ends up using.
                throw new IllegalArgumentException("Invalid '" + AdbcCatalogProperties.DRIVER_URL
                        + "': " + raw, e);
            }
            String authority = uri.getRawAuthority();
            if ((authority != null && !authority.isEmpty())
                    || uri.getRawQuery() != null || uri.getRawFragment() != null) {
                throw new IllegalArgumentException("Invalid '" + AdbcCatalogProperties.DRIVER_URL
                        + "': a file:// URL must carry no authority, query or fragment (got: " + raw + ")");
            }
            String path = uri.getPath();
            if (path == null || path.isEmpty()) {
                throw new IllegalArgumentException("Invalid '" + AdbcCatalogProperties.DRIVER_URL
                        + "': no path in " + raw);
            }
            return Paths.get(path);
        }
        if (raw.startsWith("/")) {
            return Paths.get(raw);
        }
        if (!SAFE_DRIVER_FILE_NAME.matcher(raw).matches()) {
            throw new IllegalArgumentException("Invalid '" + AdbcCatalogProperties.DRIVER_URL
                    + "': a bare driver file name must match [A-Za-z0-9._-]+.so (got: " + raw + ")."
                    + " Use an absolute path or a file:// URL to reference a driver outside "
                    + AdbcConf.CONF_DRIVERS_DIR + " (adbc.conf)");
        }
        if (driversDir == null || driversDir.trim().isEmpty()) {
            throw new IllegalArgumentException("Cannot resolve the bare driver file name '" + raw
                    + "': " + AdbcConf.CONF_DRIVERS_DIR
                    + " is not configured in adbc.conf and DORIS_HOME is unknown");
        }
        return Paths.get(driversDir.trim(), raw);
    }

    /**
     * Rejects {@code ..} on the DECODED path, so a percent-encoded parent segment cannot survive to the
     * loader. Checked before normalization, because normalization is exactly what would hide it.
     */
    private static void rejectTraversal(String raw, Path candidate) {
        for (Path segment : candidate) {
            if ("..".equals(segment.toString())) {
                throw new IllegalArgumentException("Invalid '" + AdbcCatalogProperties.DRIVER_URL
                        + "': path traversal ('..') is not allowed: " + raw);
            }
        }
    }

    /**
     * Component-based containment check, so neither prefix confusion ({@code /opt/drv} vs
     * {@code /opt/drv-evil}) nor traversal can place a driver outside an allowed directory.
     */
    private static void checkSecurePath(String raw, Path candidate, String securePath) {
        if (securePath == null || securePath.trim().isEmpty() || "*".equals(securePath.trim())) {
            return;
        }
        List<Path> allowed = new ArrayList<>();
        for (String entry : securePath.split(";")) {
            String trimmed = entry.trim();
            if (!trimmed.isEmpty()) {
                allowed.add(Paths.get(trimmed).normalize());
            }
        }
        Path normalized = candidate.normalize();
        for (Path base : allowed) {
            if (normalized.startsWith(base)) {
                return;
            }
        }
        throw new IllegalArgumentException("Driver path does not match any path allowed by "
                + AdbcConf.CONF_DRIVER_SECURE_PATH + " in adbc.conf ("
                + securePath + "): " + raw);
    }

    /**
     * Fails with a self-serviceable message when the driver file is absent.
     *
     * <p>Doris does not ship ADBC drivers, so "file not found" is the single most likely first experience
     * of this catalog type. A bare {@code dlopen failed: No such file} would leave a user with no idea what
     * to put where, so the message states all four things they need: the exact path, that FE and every BE
     * need it, and both places to obtain it.
     */
    public static void checkExists(Path driverPath, String driverUrl) {
        if (Files.isReadable(driverPath)) {
            return;
        }
        throw new IllegalArgumentException("ADBC driver library not found: " + driverPath
                + " (from '" + AdbcCatalogProperties.DRIVER_URL + "' = " + driverUrl + ")."
                + " Doris does not ship ADBC drivers. Place the driver shared library at this path on the"
                + " FE and at the matching path on EVERY BE -- the same file, because partition descriptors"
                + " do not carry across driver builds. Drivers are published on the arrow-adbc GitHub"
                + " releases page, and can also be extracted from the PyPI wheels (for Flight SQL:"
                + " adbc_driver_flightsql).");
    }

    /**
     * Fails when the driver file is not the one the catalog declared, by MD5.
     *
     * <p>Optional, and the only property here a user has to compute themselves. It earns its place because
     * of what this catalog type asks of an operator: place one file, by hand, on every node, and keep the
     * copies identical. Nothing about a wrong or stale copy announces itself -- the library still loads, and
     * whatever it does differently surfaces later as a query failure with no mention of a file.
     *
     * <p><b>Scope: the file on THIS FE.</b> It cannot see what any BE has, so it does not verify that the
     * copies match across nodes -- the failure that would hurt most. Declaring the same checksum on the
     * catalog and deploying with it is what makes that check meaningful; Doris only holds up its end here.
     */
    public static void checkChecksum(Path driverPath, String declaredChecksum, String driverUrl) {
        if (declaredChecksum == null || declaredChecksum.trim().isEmpty()) {
            return;
        }
        String actual = md5Of(driverPath, driverUrl);
        if (actual.equalsIgnoreCase(declaredChecksum.trim())) {
            return;
        }
        throw new IllegalArgumentException("The ADBC driver at " + driverPath + " has MD5 " + actual
                + ", but '" + AdbcCatalogProperties.DRIVER_CHECKSUM + "' declares "
                + declaredChecksum.trim() + " (from '" + AdbcCatalogProperties.DRIVER_URL + "' = "
                + driverUrl + "). This FE is holding a different driver build from the one the catalog was"
                + " written for; every BE must hold that same build too.");
    }

    private static String md5Of(Path driverPath, String driverUrl) {
        try (InputStream in = Files.newInputStream(driverPath)) {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) >= 0) {
                digest.update(buffer, 0, read);
            }
            StringBuilder hex = new StringBuilder(32);
            for (byte b : digest.digest()) {
                hex.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
            }
            return hex.toString();
        } catch (IOException | NoSuchAlgorithmException e) {
            // Fail closed: a checksum that could not be computed has verified nothing, and treating that as
            // a pass would make the property quietly optional on exactly the nodes where reading fails.
            throw new IllegalArgumentException("Cannot compute the MD5 of the ADBC driver at " + driverPath
                    + " (from '" + AdbcCatalogProperties.DRIVER_URL + "' = " + driverUrl + "): "
                    + e.getMessage(), e);
        }
    }
}
