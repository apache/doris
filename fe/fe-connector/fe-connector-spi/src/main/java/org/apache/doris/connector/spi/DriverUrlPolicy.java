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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The FE's policy for a driver jar a connector is about to load into the FE JVM: which {@code driver_url}
 * forms are accepted, where a bare jar name resolves, and which locations the deployment allows.
 *
 * <p>One implementation, applied by every connector that loads a driver jar (the jdbc connector for its
 * own driver, the iceberg and paimon connectors for their JDBC-backed metastores), so the three cannot
 * drift apart and the engine carries none of it. The inputs are the FE settings the engine forwards through
 * {@link ConnectorContext#getEnvironment()} — {@code jdbc_drivers_dir}, {@code doris_home},
 * {@code jdbc_driver_secure_path}, {@code jdbc_driver_url_white_list} — bundled into a {@link Settings}.</p>
 *
 * <p>{@link #resolve} is the whole rule, unchanged from the engine-side resolver it replaces:</p>
 * <ol>
 *   <li>the url must be {@code file://…}, {@code http://…}, {@code https://…}, or a bare {@code name.jar};
 *       anything else — including a url {@link URI} cannot parse — is rejected (fail closed, so a malformed
 *       url can never slip past the location checks below);</li>
 *   <li>when {@code jdbc_driver_url_white_list} is set, the url must be listed verbatim;</li>
 *   <li>a bare jar name resolves under the drivers directory: the file there, else (when that directory is
 *       the default one) the pre-2.1 default {@code DORIS_HOME/jdbc_drivers}, else the deployment's external
 *       plugin store ({@link Settings#getMissingFileFetcher()}), else "does not exist";</li>
 *   <li>a scheme-bearing url must sit under one of the {@code jdbc_driver_secure_path} prefixes, matched
 *       structurally (component-based) so that neither prefix confusion ({@code /opt/drivers} vs
 *       {@code /opt/drivers-evil}) nor path traversal can escape the allowed location; {@code "*"} or blank
 *       allows all.</li>
 * </ol>
 */
public final class DriverUrlPolicy {

    /** Timeout for both connecting and reading a driver jar for its checksum. 10 seconds is long enough. */
    private static final int HTTP_TIMEOUT_MS = 10000;

    /** Category name of driver jars in the deployment's external plugin store. */
    public static final String PLUGIN_FILE_CATEGORY_JDBC_DRIVERS = "jdbc_drivers";

    /** Environment key: the directory a bare {@code driver_url} resolves under (fe.conf {@code jdbc_drivers_dir}). */
    public static final String ENV_DRIVERS_DIR = "jdbc_drivers_dir";
    /** Environment key: the FE install root. */
    public static final String ENV_DORIS_HOME = "doris_home";
    /** Environment key: fe.conf {@code jdbc_driver_secure_path}, semicolon-separated prefixes; "*" or blank = all. */
    public static final String ENV_DRIVER_SECURE_PATH = "jdbc_driver_secure_path";
    /** Environment key: fe.conf {@code jdbc_driver_url_white_list}, comma-separated exact urls; blank = unset. */
    public static final String ENV_DRIVER_URL_WHITE_LIST = "jdbc_driver_url_white_list";

    private DriverUrlPolicy() {
    }

    /**
     * Fetches a bare-named jar that is absent from the local drivers directory from the deployment's external
     * plugin store. Answers the local path of the copy, or empty when the deployment has no such store.
     * Adapts {@link ConnectorContext#fetchPluginFile} for {@link #PLUGIN_FILE_CATEGORY_JDBC_DRIVERS}.
     */
    @FunctionalInterface
    public interface MissingFileFetcher {
        Optional<String> fetch(String fileName, String targetPath);
    }

    /** The FE-level inputs of the policy. Build one from the connector's context with {@link #fromContext}. */
    public static final class Settings {
        private final String driversDir;
        private final String dorisHome;
        private final String securePath;
        private final List<String> urlWhiteList;
        private final MissingFileFetcher missingFileFetcher;

        /**
         * @param driversDir         directory a bare jar name resolves under; blank falls back to
         *                           {@code <dorisHome>/plugins/jdbc_drivers}
         * @param dorisHome          the FE install root; blank means "."
         * @param securePath         semicolon-separated allowed prefixes; {@code "*"}, blank or null allows all
         * @param urlWhiteList       exact urls allowed; empty means no white list
         * @param missingFileFetcher the external plugin store, or null when the deployment has none
         */
        public Settings(String driversDir, String dorisHome, String securePath, List<String> urlWhiteList,
                MissingFileFetcher missingFileFetcher) {
            this.driversDir = driversDir;
            this.dorisHome = dorisHome;
            this.securePath = securePath;
            List<String> whiteList = new ArrayList<>();
            if (urlWhiteList != null) {
                for (String entry : urlWhiteList) {
                    if (entry != null && !entry.isEmpty()) {
                        whiteList.add(entry);
                    }
                }
            }
            this.urlWhiteList = Collections.unmodifiableList(whiteList);
            this.missingFileFetcher = missingFileFetcher;
        }

        /**
         * Reads the FE settings off the connector's context. {@code driversDir} is passed in rather than read
         * here because which settings file it comes from is the connector's business (its own {@code <name>.conf}
         * first, then fe.conf's {@code jdbc_drivers_dir}); the other three are engine-wide and stay in the
         * environment. The external plugin store is {@link ConnectorContext#fetchPluginFile}.
         */
        public static Settings fromContext(ConnectorContext context, String driversDir) {
            Map<String, String> env = context.getEnvironment() == null
                    ? Collections.emptyMap() : context.getEnvironment();
            String whiteList = env.get(ENV_DRIVER_URL_WHITE_LIST);
            List<String> urls = whiteList == null || whiteList.trim().isEmpty()
                    ? Collections.emptyList() : Arrays.asList(whiteList.split(","));
            return new Settings(driversDir, env.get(ENV_DORIS_HOME), env.get(ENV_DRIVER_SECURE_PATH), urls,
                    (fileName, targetPath) ->
                            context.fetchPluginFile(PLUGIN_FILE_CATEGORY_JDBC_DRIVERS, fileName, targetPath));
        }

        public String getDriversDir() {
            return driversDir;
        }

        public String getDorisHome() {
            return dorisHome;
        }

        public String getSecurePath() {
            return securePath;
        }

        public List<String> getUrlWhiteList() {
            return urlWhiteList;
        }

        public MissingFileFetcher getMissingFileFetcher() {
            return missingFileFetcher;
        }

        /** The directory a bare jar name resolves under, with the default applied. */
        String effectiveDriversDir() {
            if (driversDir != null && !driversDir.trim().isEmpty()) {
                return driversDir;
            }
            return defaultDriversDir();
        }

        /** {@code <dorisHome>/plugins/jdbc_drivers}: the drivers directory of a deployment that configured none. */
        String defaultDriversDir() {
            String home = dorisHome == null || dorisHome.trim().isEmpty() ? "." : dorisHome;
            return home + "/plugins/jdbc_drivers";
        }

        /** {@code <dorisHome>/jdbc_drivers}, the drivers directory of releases before 2.1, still consulted. */
        String legacyDriversDir() {
            String home = dorisHome == null || dorisHome.trim().isEmpty() ? "." : dorisHome;
            return home + "/jdbc_drivers";
        }
    }

    /**
     * Validates {@code driverUrl} against the policy and resolves it to the full, scheme-bearing url the
     * driver is loaded from.
     *
     * @throws IllegalArgumentException when the url is malformed or outside the allowed locations
     * @throws RuntimeException when a bare jar name exists nowhere the policy looks (message names the file),
     *                          or the external plugin store fails to deliver it
     */
    public static String resolve(String driverUrl, Settings settings) {
        Objects.requireNonNull(driverUrl, "driverUrl");
        Objects.requireNonNull(settings, "settings");
        if (!(driverUrl.startsWith("file://") || driverUrl.startsWith("http://")
                || driverUrl.startsWith("https://") || driverUrl.matches("^[^:/]+\\.jar$"))) {
            throw new IllegalArgumentException("Invalid driver URL format. Supported formats are: "
                    + "file://xxx.jar, http://xxx.jar, https://xxx.jar, or xxx.jar (without prefix).");
        }

        URI uri;
        try {
            uri = new URI(driverUrl);
        } catch (URISyntaxException e) {
            // Fail closed: an unparsable URL must never be silently accepted, otherwise the
            // allowed-path check below could be bypassed by a malformed URL.
            throw new IllegalArgumentException("Invalid driver URL: " + driverUrl, e);
        }

        String schema = uri.getScheme();
        checkWhiteList(driverUrl, settings);
        if (schema == null && !driverUrl.startsWith("/")) {
            // A scheme-less driver_url is a plain jar file name resolved under the drivers directory. This
            // resolver is also on the lazy load path of pre-existing catalogs (with no create/alter or replay
            // context), so it deliberately applies no new restriction here: an unmodified historical catalog
            // must keep resolving exactly as before. The mandatory bare-name grammar is enforced only when a
            // catalog is created or altered, by the jdbc connector's checkDriverUrlSecurityRule.
            return resolveBareName(driverUrl, settings);
        }

        // "*" or an empty/blank value means allow all (the documented, backward-compatible contract).
        String securePath = settings.getSecurePath();
        if (securePath == null || securePath.trim().isEmpty() || "*".equals(securePath.trim())) {
            return driverUrl;
        }

        if (!isDriverUrlAllowed(driverUrl, uri, securePath)) {
            throw new IllegalArgumentException("Driver URL does not match any allowed paths: " + driverUrl);
        }
        return driverUrl;
    }

    /**
     * The MD5 of the jar at {@code fullDriverUrl} (as returned by {@link #resolve}), hex-encoded. A remote url
     * goes through {@code hook} first, which is how the engine's outbound-request policy (SSRF checks) applies
     * to a jar the connector fetches itself.
     */
    public static String checksum(String fullDriverUrl, ConnectorHttpSecurityHook hook) throws IOException {
        Objects.requireNonNull(fullDriverUrl, "fullDriverUrl");
        boolean remote = !(fullDriverUrl.startsWith("/") || fullDriverUrl.startsWith("file://"));
        ConnectorHttpSecurityHook effectiveHook = hook == null ? ConnectorHttpSecurityHook.NOOP : hook;
        try {
            if (remote) {
                effectiveHook.beforeRequest(fullDriverUrl);
            }
            URLConnection conn = new URL(fullDriverUrl).openConnection();
            conn.setConnectTimeout(HTTP_TIMEOUT_MS);
            conn.setReadTimeout(HTTP_TIMEOUT_MS);
            try (InputStream inputStream = conn.getInputStream()) {
                MessageDigest digest = MessageDigest.getInstance("MD5");
                byte[] buf = new byte[4096];
                int bytesRead;
                while ((bytesRead = inputStream.read(buf)) >= 0) {
                    digest.update(buf, 0, bytesRead);
                }
                return toHex(digest.digest());
            }
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("could not find algorithm: " + e.getMessage(), e);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        } finally {
            if (remote) {
                effectiveHook.afterRequest();
            }
        }
    }

    private static void checkWhiteList(String driverUrl, Settings settings) {
        // For compatibility with cloud mode, both `jdbc_driver_url_white_list` and
        // `jdbc_driver_secure_path` gate a driver url.
        List<String> whiteList = settings.getUrlWhiteList();
        if (!whiteList.isEmpty() && !whiteList.contains(driverUrl)) {
            throw new IllegalArgumentException("Driver URL does not match any allowed paths" + driverUrl);
        }
    }

    private static String resolveBareName(String driverUrl, Settings settings) {
        String driversDir = settings.effectiveDriversDir();
        if (!driversDir.equals(settings.defaultDriversDir())) {
            // The deployment configured its own drivers directory: resolve there, nothing else to consult.
            return "file://" + driversDir + "/" + driverUrl;
        }
        // The default directory is in use. Its location moved from DORIS_HOME/jdbc_drivers to
        // DORIS_HOME/plugins/jdbc_drivers, so the old location is still consulted for jars that never moved.
        String targetPath = driversDir + "/" + driverUrl;
        if (new File(targetPath).exists()) {
            return "file://" + targetPath;
        }
        String oldTargetPath = settings.legacyDriversDir() + "/" + driverUrl;
        if (new File(oldTargetPath).exists()) {
            return "file://" + oldTargetPath;
        }
        MissingFileFetcher fetcher = settings.getMissingFileFetcher();
        if (fetcher != null) {
            Optional<String> fetched;
            try {
                fetched = fetcher.fetch(driverUrl, targetPath);
            } catch (Exception e) {
                throw new RuntimeException("Cannot download JDBC driver from cloud: " + driverUrl
                        + ". Please retry later or check your driver has been uploaded to cloud. Error: "
                        + rootCauseMessage(e), e);
            }
            if (fetched != null && fetched.isPresent()) {
                return "file://" + fetched.get();
            }
        }
        throw new RuntimeException("JDBC driver file does not exist: " + driverUrl);
    }

    /**
     * Whether {@code driverUrl} falls under one of the semicolon-separated prefixes in {@code securePath}.
     * Matching is structural (component-based) rather than a raw string prefix, so that neither prefix
     * confusion ({@code /opt/drivers} vs {@code /opt/drivers-evil}) nor path traversal
     * ({@code /opt/drivers/../etc}) can slip a driver outside the allowed location.
     */
    private static boolean isDriverUrlAllowed(String driverUrl, URI uri, String securePath) {
        String scheme = uri.getScheme();
        List<String> allowedPaths = new ArrayList<>();
        for (String p : securePath.split(";")) {
            String trimmed = p.trim();
            if (!trimmed.isEmpty()) {
                allowedPaths.add(trimmed);
            }
        }
        if ("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme)) {
            URI candidate = uri.normalize();
            return allowedPaths.stream().anyMatch(allowed -> remoteUrlMatches(candidate, allowed));
        }
        // Only file:// reaches here; bare absolute paths and bare "*.jar" are handled earlier.
        // A local file URL must carry no authority, query or fragment. Otherwise validation (which
        // looks only at URI.getPath()) and the consumers (URLClassLoader / checksum, which act on the
        // whole original URL) would address different objects — e.g. "file://attacker/dir/x.jar" is
        // fetched from a remote authority, and "file:///dir/x.jar?evil" maps to a sibling file.
        String authority = uri.getRawAuthority();
        if ((authority != null && !authority.isEmpty())
                || uri.getRawQuery() != null || uri.getRawFragment() != null) {
            return false;
        }
        Path candidate = toLocalPath(driverUrl).normalize();
        return allowedPaths.stream()
                .map(allowed -> toLocalPath(allowed).normalize())
                .anyMatch(candidate::startsWith);
    }

    /**
     * Turns a {@code file://} URL or a plain filesystem path into a {@link Path} for structural comparison.
     * A {@code file://} URL is decoded exactly once via {@link URI#getPath()} so that percent-encoded
     * segments (e.g. {@code %2e%2e}) are resolved into the same representation the driver-loading
     * consumers ({@code URL.openStream} / {@code URLClassLoader}) will use; otherwise an encoded parent
     * segment would survive normalization and escape the allowed directory.
     */
    private static Path toLocalPath(String pathOrUrl) {
        if (pathOrUrl.startsWith("file:")) {
            try {
                String decoded = new URI(pathOrUrl).getPath();
                if (decoded != null && !decoded.isEmpty()) {
                    return Paths.get(decoded);
                }
            } catch (URISyntaxException ignored) {
                // fall through to literal stripping below
            }
            int sep = pathOrUrl.indexOf("//");
            return Paths.get(sep >= 0 ? pathOrUrl.substring(sep + 2) : pathOrUrl.substring("file:".length()));
        }
        return Paths.get(pathOrUrl);
    }

    /**
     * Structural match for remote (http/https) driver URLs: scheme, host and port must be equal, and the
     * candidate path must sit under the allowed path (component-based). A bare path prefix (no scheme) can
     * never authorize a remote URL.
     */
    private static boolean remoteUrlMatches(URI candidate, String allowedPath) {
        URI base;
        try {
            base = new URI(allowedPath).normalize();
        } catch (URISyntaxException e) {
            return false;
        }
        if (base.getScheme() == null) {
            return false;
        }
        // Scheme/host/port and the path prefix must match, and the resource-selecting components
        // (user-info and query) that the checksum/classloader consumers act on must match exactly too,
        // otherwise e.g. ".../download?id=approved" would authorize ".../download?id=evil".
        return base.getScheme().equalsIgnoreCase(candidate.getScheme())
                && base.getHost() != null && base.getHost().equalsIgnoreCase(candidate.getHost())
                && base.getPort() == candidate.getPort()
                && Objects.equals(base.getUserInfo(), candidate.getUserInfo())
                && Objects.equals(base.getRawQuery(), candidate.getRawQuery())
                && pathIsUnder(candidate.getPath(), base.getPath());
    }

    private static boolean pathIsUnder(String candidatePath, String basePath) {
        Path candidate = Paths.get(candidatePath == null || candidatePath.isEmpty() ? "/" : candidatePath).normalize();
        Path base = Paths.get(basePath == null || basePath.isEmpty() ? "/" : basePath).normalize();
        return candidate.startsWith(base);
    }

    private static String rootCauseMessage(Throwable t) {
        Throwable p = t;
        while (p.getCause() != null) {
            p = p.getCause();
        }
        return p.getMessage() == null ? p.getClass().getName() : p.getMessage();
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString().toLowerCase(Locale.ROOT);
    }
}
