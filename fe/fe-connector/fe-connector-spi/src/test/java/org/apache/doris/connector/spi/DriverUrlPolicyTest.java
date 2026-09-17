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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The driver-url policy, case for case as the engine-side resolver it replaced ({@code JdbcResource
 * .getFullDriverUrl}) was tested: url grammar, the url white list, structural secure-path matching for local
 * and remote urls, bare-name resolution across the current and pre-2.1 default directories, and the external
 * plugin store fallback.
 */
public class DriverUrlPolicyTest {

    private static DriverUrlPolicy.Settings settings(String securePath) {
        return new DriverUrlPolicy.Settings("/opt/doris/plugins/jdbc_drivers", "/opt/doris", securePath,
                Collections.emptyList(), null);
    }

    private static DriverUrlPolicy.Settings allowAll() {
        return settings("*");
    }

    @Test
    public void validUrlsPassThroughUnchanged() {
        for (String url : new String[] {"file://path/to/driver.jar", "http://example.com/driver.jar",
                "https://example.com/driver.jar"}) {
            Assertions.assertEquals(url, DriverUrlPolicy.resolve(url, allowAll()));
        }
    }

    @Test
    public void bareNameMissingEverywhereIsReported() {
        RuntimeException e = Assertions.assertThrows(RuntimeException.class,
                () -> DriverUrlPolicy.resolve("driver.jar", allowAll()));
        Assertions.assertTrue(e.getMessage().contains("JDBC driver file does not exist: driver.jar"),
                e.getMessage());
    }

    @Test
    public void malformedUrlsAreRejected() {
        for (String url : new String[] {"/mnt/path/to/driver.jar", "ftp://example.com/driver.jar", "",
                "example.com/driver"}) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> DriverUrlPolicy.resolve(url, allowAll()), url);
        }
    }

    @Test
    public void unparsableUrlFailsClosed() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> DriverUrlPolicy.resolve("http://exa mple.com/driver.jar", allowAll()));
    }

    @Test
    public void whiteListMustListTheUrlVerbatim() {
        DriverUrlPolicy.Settings listed = new DriverUrlPolicy.Settings(null, null, "*",
                List.of("http://good.com/a.jar", ""), null);
        Assertions.assertEquals("http://good.com/a.jar", DriverUrlPolicy.resolve("http://good.com/a.jar", listed));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> DriverUrlPolicy.resolve("http://good.com/b.jar", listed));
        // An all-blank white list is no white list.
        DriverUrlPolicy.Settings blank = new DriverUrlPolicy.Settings(null, null, "*", List.of("", ""), null);
        Assertions.assertEquals("http://good.com/b.jar", DriverUrlPolicy.resolve("http://good.com/b.jar", blank));
    }

    @Test
    public void securePathRejectsPrefixConfusion() {
        // A directory that merely shares a string prefix must NOT be allowed.
        Assertions.assertThrows(IllegalArgumentException.class, () -> DriverUrlPolicy.resolve(
                "file:///opt/doris/jdbc_drivers-evil/x.jar", settings("file:///opt/doris/jdbc_drivers")));
    }

    @Test
    public void securePathRejectsPathTraversal() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> DriverUrlPolicy.resolve(
                "file:///opt/doris/jdbc_drivers/../../etc/x.jar", settings("file:///opt/doris/jdbc_drivers")));
    }

    @Test
    public void securePathAllowsPathUnderAllowedDir() {
        String url = "file:///opt/doris/jdbc_drivers/sub/x.jar";
        Assertions.assertEquals(url, DriverUrlPolicy.resolve(url, settings("file:///opt/doris/jdbc_drivers")));
    }

    @Test
    public void securePathAcceptsSemicolonSeparatedPrefixes() {
        String url = "file:///var/lib/drivers/x.jar";
        Assertions.assertEquals(url, DriverUrlPolicy.resolve(url,
                settings("file:///opt/doris/jdbc_drivers; file:///var/lib/drivers ;")));
    }

    @Test
    public void securePathRejectsHostConfusion() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> DriverUrlPolicy.resolve(
                "http://good.com.evil.com/x.jar", settings("http://good.com/")));
    }

    @Test
    public void securePathAllowsRemoteUnderAllowedHost() {
        String url = "http://good.com/drivers/x.jar";
        Assertions.assertEquals(url, DriverUrlPolicy.resolve(url, settings("http://good.com/drivers")));
    }

    @Test
    public void securePathWildcardAndBlankAllowAll() {
        String url = "file:///any/where/x.jar";
        Assertions.assertEquals(url, DriverUrlPolicy.resolve(url, settings("*")));
        Assertions.assertEquals(url, DriverUrlPolicy.resolve(url, settings("")));
        Assertions.assertEquals(url, DriverUrlPolicy.resolve(url, settings(null)));
    }

    @Test
    public void securePathRejectsEncodedTraversal() {
        // %2e%2e decodes to "..", which must be resolved the same way the classloader resolves it.
        Assertions.assertThrows(IllegalArgumentException.class, () -> DriverUrlPolicy.resolve(
                "file:///opt/doris/jdbc_drivers/%2e%2e/%2e%2e/etc/x.jar", settings("file:///opt/doris/jdbc_drivers")));
    }

    @Test
    public void securePathRejectsRemoteQueryMismatch() {
        // A query-bearing URL must not be authorized by a query-less allowed prefix.
        Assertions.assertThrows(IllegalArgumentException.class, () -> DriverUrlPolicy.resolve(
                "http://good.com/drivers/x.jar?id=evil", settings("http://good.com/drivers")));
    }

    @Test
    public void securePathRejectsRemoteUserInfoMismatch() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> DriverUrlPolicy.resolve(
                "http://user@good.com/drivers/x.jar", settings("http://good.com/drivers")));
    }

    @Test
    public void securePathRejectsFileAuthority() {
        // A non-local authority makes consumers fetch a remote object though the path matches.
        Assertions.assertThrows(IllegalArgumentException.class, () -> DriverUrlPolicy.resolve(
                "file://attacker.example/opt/doris/jdbc_drivers/evil.jar", settings("file:///opt/doris/jdbc_drivers")));
    }

    @Test
    public void securePathRejectsFileQuery() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> DriverUrlPolicy.resolve(
                "file:///opt/doris/jdbc_drivers/x.jar?evil", settings("file:///opt/doris/jdbc_drivers")));
    }

    @Test
    public void bareNameResolvesUnderCustomDriversDirWithoutLookingAtTheFile() {
        // A deployment-configured directory is authoritative: the name is resolved there, historically
        // valid characters (e.g. '+') included, and no new restriction applies on this lazy load path.
        DriverUrlPolicy.Settings custom = new DriverUrlPolicy.Settings("/opt/doris/jdbc_drivers", "/opt/doris",
                "*", Collections.emptyList(), null);
        Assertions.assertEquals("file:///opt/doris/jdbc_drivers/legacy+patched.jar",
                DriverUrlPolicy.resolve("legacy+patched.jar", custom));
    }

    @Test
    public void bareNameResolvesInTheDefaultDirThenThePre21Dir(@TempDir Path home) throws IOException {
        Path newDir = Files.createDirectories(home.resolve("plugins/jdbc_drivers"));
        Path oldDir = Files.createDirectories(home.resolve("jdbc_drivers"));
        Files.write(newDir.resolve("new.jar"), new byte[] {1});
        Files.write(oldDir.resolve("old.jar"), new byte[] {2});
        DriverUrlPolicy.Settings defaults = new DriverUrlPolicy.Settings(newDir.toString(), home.toString(), "*",
                Collections.emptyList(), null);

        Assertions.assertEquals("file://" + newDir.resolve("new.jar"), DriverUrlPolicy.resolve("new.jar", defaults));
        Assertions.assertEquals("file://" + oldDir.resolve("old.jar"), DriverUrlPolicy.resolve("old.jar", defaults));
        Assertions.assertThrows(RuntimeException.class, () -> DriverUrlPolicy.resolve("none.jar", defaults));
    }

    @Test
    public void bareNameFallsBackToTheExternalPluginStore(@TempDir Path home) throws IOException {
        Path newDir = Files.createDirectories(home.resolve("plugins/jdbc_drivers"));
        List<String> requests = new ArrayList<>();
        DriverUrlPolicy.Settings cloud = new DriverUrlPolicy.Settings(newDir.toString(), home.toString(), "*",
                Collections.emptyList(), (name, target) -> {
                    requests.add(name + "->" + target);
                    return Optional.of(target);
                });
        Assertions.assertEquals("file://" + newDir.resolve("cloud.jar"), DriverUrlPolicy.resolve("cloud.jar", cloud));
        Assertions.assertEquals(List.of("cloud.jar->" + newDir.resolve("cloud.jar")), requests);

        DriverUrlPolicy.Settings failing = new DriverUrlPolicy.Settings(newDir.toString(), home.toString(), "*",
                Collections.emptyList(), (name, target) -> {
                    throw new IllegalStateException("bucket unreachable");
                });
        RuntimeException e = Assertions.assertThrows(RuntimeException.class,
                () -> DriverUrlPolicy.resolve("cloud.jar", failing));
        Assertions.assertTrue(e.getMessage().contains("has been uploaded to cloud"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("bucket unreachable"), e.getMessage());

        DriverUrlPolicy.Settings noStore = new DriverUrlPolicy.Settings(newDir.toString(), home.toString(), "*",
                Collections.emptyList(), (name, target) -> Optional.empty());
        Assertions.assertThrows(RuntimeException.class, () -> DriverUrlPolicy.resolve("cloud.jar", noStore));
    }

    @Test
    public void settingsFromContextReadTheEngineEnvironment() {
        Map<String, String> env = new HashMap<>();
        env.put(DriverUrlPolicy.ENV_DORIS_HOME, "/opt/doris");
        env.put(DriverUrlPolicy.ENV_DRIVER_SECURE_PATH, "file:///opt/doris/jdbc_drivers");
        env.put(DriverUrlPolicy.ENV_DRIVER_URL_WHITE_LIST, "http://good.com/a.jar,http://good.com/b.jar");
        List<String> fetches = new ArrayList<>();
        ConnectorContext context = new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "c";
            }

            @Override
            public long getCatalogId() {
                return 1;
            }

            @Override
            public Map<String, String> getEnvironment() {
                return env;
            }

            @Override
            public Optional<String> fetchPluginFile(String category, String fileName, String targetPath) {
                fetches.add(category + ":" + fileName);
                return Optional.empty();
            }
        };
        DriverUrlPolicy.Settings settings = DriverUrlPolicy.Settings.fromContext(context, "/custom/drivers");
        Assertions.assertEquals("/custom/drivers", settings.getDriversDir());
        Assertions.assertEquals("/opt/doris", settings.getDorisHome());
        Assertions.assertEquals("file:///opt/doris/jdbc_drivers", settings.getSecurePath());
        Assertions.assertEquals(List.of("http://good.com/a.jar", "http://good.com/b.jar"), settings.getUrlWhiteList());
        settings.getMissingFileFetcher().fetch("x.jar", "/tmp/x.jar");
        Assertions.assertEquals(List.of(DriverUrlPolicy.PLUGIN_FILE_CATEGORY_JDBC_DRIVERS + ":x.jar"), fetches);

        DriverUrlPolicy.Settings unset = DriverUrlPolicy.Settings.fromContext(context, null);
        Assertions.assertEquals("/opt/doris/plugins/jdbc_drivers", unset.effectiveDriversDir());
    }

    @Test
    public void checksumIsTheHexMd5OfTheFile(@TempDir Path dir) throws IOException {
        Path jar = dir.resolve("d.jar");
        Files.write(jar, "hello".getBytes(StandardCharsets.UTF_8));
        Assertions.assertEquals("5d41402abc4b2a76b9719d911017c592",
                DriverUrlPolicy.checksum(jar.toUri().toString(), null));
        Assertions.assertThrows(IOException.class,
                () -> DriverUrlPolicy.checksum(dir.resolve("missing.jar").toUri().toString(), null));
    }

    @Test
    public void checksumRoutesRemoteUrlsThroughTheSecurityHook() {
        List<String> events = new ArrayList<>();
        ConnectorHttpSecurityHook hook = new ConnectorHttpSecurityHook() {
            @Override
            public void beforeRequest(String url) {
                events.add("before:" + url);
                throw new IllegalStateException("blocked");
            }

            @Override
            public void afterRequest() {
                events.add("after");
            }
        };
        IOException e = Assertions.assertThrows(IOException.class,
                () -> DriverUrlPolicy.checksum("http://127.0.0.1:1/x.jar", hook));
        Assertions.assertTrue(e.getMessage().contains("blocked"), e.getMessage());
        Assertions.assertEquals(List.of("before:http://127.0.0.1:1/x.jar", "after"), events);
    }
}
