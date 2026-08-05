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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Rules for turning {@code driver_url} into a driver library path.
 *
 * <p>Each rejection case below exists because letting it through has a concrete consequence, stated per
 * test. The rejections matter more than the acceptances: this resolver is the only thing standing between
 * a catalog property and a {@code dlopen} of an arbitrary file into the FE process.
 */
class AdbcDriverPathResolverTest {

    private static final String DRIVERS_DIR = "/opt/doris/plugins/adbc_drivers";
    private static final String ALLOW_ALL = "*";

    private static Path resolve(String driverUrl) {
        return AdbcDriverPathResolver.resolve(driverUrl, DRIVERS_DIR, ALLOW_ALL);
    }

    private static String rejectionOf(String driverUrl, String securePath) {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> AdbcDriverPathResolver.resolve(driverUrl, DRIVERS_DIR, securePath));
        return e.getMessage();
    }

    // ---- accepted forms ----

    @Test
    void bareFileNameResolvesUnderTheDriversDirectory() {
        Assertions.assertEquals(Paths.get(DRIVERS_DIR, "libadbc_driver_flightsql.so"),
                resolve("libadbc_driver_flightsql.so"));
    }

    @Test
    void versionedSonameIsAccepted() {
        // Drivers extracted from a release tarball routinely carry an soname suffix; rejecting it would
        // force users to rename the file for no reason.
        Assertions.assertEquals(Paths.get(DRIVERS_DIR, "libadbc_driver_sqlite.so.112.0.0"),
                resolve("libadbc_driver_sqlite.so.112.0.0"));
    }

    @Test
    void fileUrlResolvesToItsPath() {
        Assertions.assertEquals(Paths.get("/data/drivers/libadbc_driver_postgresql.so"),
                resolve("file:///data/drivers/libadbc_driver_postgresql.so"));
    }

    @Test
    void absolutePathIsTakenAsIs() {
        Assertions.assertEquals(Paths.get("/data/drivers/libadbc_driver_postgresql.so"),
                resolve("/data/drivers/libadbc_driver_postgresql.so"));
    }

    @Test
    void surroundingWhitespaceIsTrimmed() {
        Assertions.assertEquals(Paths.get(DRIVERS_DIR, "libadbc_driver_flightsql.so"),
                resolve("  libadbc_driver_flightsql.so  "));
    }

    // ---- rejected forms ----

    @Test
    void remoteSchemesAreRejectedWithTheReason() {
        // Downloading per node cannot guarantee FE and every BE end up with the identical library, and a
        // mismatch surfaces as an unreadable partition descriptor -- far from its cause. So the message
        // has to explain the constraint, not just say "unsupported".
        for (String url : new String[] {
                "http://example.com/libadbc_driver_flightsql.so",
                "https://example.com/libadbc_driver_flightsql.so",
                "s3://bucket/libadbc_driver_flightsql.so"}) {
            String message = rejectionOf(url, ALLOW_ALL);
            Assertions.assertTrue(message.contains("only a local file"),
                    "must say local-only: " + message);
            Assertions.assertTrue(message.contains("every BE"),
                    "must explain that FE and every BE need the same file: " + message);
        }
    }

    @Test
    void plainTraversalIsRejected() {
        String message = rejectionOf("file:///opt/doris/plugins/adbc_drivers/../../../etc/evil.so", ALLOW_ALL);
        Assertions.assertTrue(message.contains("path traversal"), message);
    }

    @Test
    void percentEncodedTraversalIsRejected() {
        // The URL is decoded once before the check, exactly as the loader will decode it. Checking the raw
        // string instead would let %2e%2e through and land the dlopen outside the allowed directory.
        String message = rejectionOf(
                "file:///opt/doris/plugins/adbc_drivers/%2e%2e/%2e%2e/etc/evil.so", ALLOW_ALL);
        Assertions.assertTrue(message.contains("path traversal"), message);
    }

    @Test
    void bareNameWithASeparatorIsRejected() {
        // A bare name is the one form resolved relative to a directory, so any separator in it would be an
        // escape hatch out of that directory.
        String message = rejectionOf("../../etc/evil.so", ALLOW_ALL);
        Assertions.assertTrue(message.contains("bare driver file name"), message);
    }

    @Test
    void bareNameThatIsNotALibraryIsRejected() {
        String message = rejectionOf("driver.jar", ALLOW_ALL);
        Assertions.assertTrue(message.contains("bare driver file name"), message);
    }

    @Test
    void fileUrlWithAnAuthorityIsRejected() {
        // "file://attacker/dir/x.so" carries a remote authority that URI.getPath() does not show, so
        // validating the path alone would authorize an object the loader would fetch from elsewhere.
        String message = rejectionOf("file://attacker/dir/libadbc_driver_flightsql.so", ALLOW_ALL);
        Assertions.assertTrue(message.contains("no authority, query or fragment"), message);
    }

    @Test
    void fileUrlWithAQueryIsRejected() {
        String message = rejectionOf("file:///dir/libadbc_driver_flightsql.so?x=1", ALLOW_ALL);
        Assertions.assertTrue(message.contains("no authority, query or fragment"), message);
    }

    @Test
    void missingDriverUrlNamesTheProperty() {
        for (String value : new String[] {null, "", "   "}) {
            String message = rejectionOf(value, ALLOW_ALL);
            Assertions.assertTrue(message.contains("driver_url"), message);
        }
    }

    // ---- secure path ----

    @Test
    void securePathAllowsWhatIsUnderIt() {
        Assertions.assertEquals(Paths.get("/opt/drv/libadbc_driver_flightsql.so"),
                AdbcDriverPathResolver.resolve("/opt/drv/libadbc_driver_flightsql.so",
                        DRIVERS_DIR, "/opt/drv;/srv/drv"));
        Assertions.assertEquals(Paths.get("/srv/drv/sub/libadbc_driver_flightsql.so"),
                AdbcDriverPathResolver.resolve("/srv/drv/sub/libadbc_driver_flightsql.so",
                        DRIVERS_DIR, "/opt/drv;/srv/drv"));
    }

    @Test
    void securePathRejectsWhatIsOutsideIt() {
        String message = rejectionOf("/etc/libadbc_driver_flightsql.so", "/opt/drv");
        Assertions.assertTrue(message.contains("driver_secure_path"), message);
    }

    @Test
    void securePathIsMatchedByComponentNotByStringPrefix() {
        // "/opt/drv-evil" starts with the string "/opt/drv" but is a different directory. A raw prefix
        // check would authorize it.
        String message = rejectionOf("/opt/drv-evil/libadbc_driver_flightsql.so", "/opt/drv");
        Assertions.assertTrue(message.contains("driver_secure_path"), message);
    }

    @Test
    void starAndBlankSecurePathAllowEverything() {
        for (String securePath : new String[] {"*", "", "   ", null}) {
            Assertions.assertEquals(Paths.get("/anywhere/libadbc_driver_flightsql.so"),
                    AdbcDriverPathResolver.resolve("/anywhere/libadbc_driver_flightsql.so",
                            DRIVERS_DIR, securePath));
        }
    }

    // ---- existence ----

    @Test
    void missingDriverFileProducesASelfServiceableMessage(@TempDir Path tempDir) {
        Path absent = tempDir.resolve("libadbc_driver_flightsql.so");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> AdbcDriverPathResolver.checkExists(absent, "libadbc_driver_flightsql.so"));
        String message = e.getMessage();
        // Doris ships no ADBC driver, so this is the most likely first experience of the catalog type;
        // the message has to answer all of "which path", "who else needs it" and "where do I get it".
        Assertions.assertTrue(message.contains(absent.toString()), message);
        Assertions.assertTrue(message.contains("EVERY BE"), message);
        Assertions.assertTrue(message.contains("arrow-adbc"), message);
        Assertions.assertTrue(message.contains("adbc_driver_flightsql"), message);
    }

    @Test
    void presentDriverFilePasses(@TempDir Path tempDir) throws Exception {
        Path present = Files.createFile(tempDir.resolve("libadbc_driver_flightsql.so"));
        Assertions.assertDoesNotThrow(
                () -> AdbcDriverPathResolver.checkExists(present, "libadbc_driver_flightsql.so"));
    }

    // ---- checksum ----

    /** {@code md5sum} of the three bytes below, which is what a user would paste into the property. */
    private static final String ABC_MD5 = "900150983cd24fb0d6963f7d28e17f72";

    private static Path driverFileContaining(Path tempDir, String content) throws Exception {
        Path file = tempDir.resolve("libadbc_driver_flightsql.so");
        Files.write(file, content.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return file;
    }

    @Test
    void declaredChecksumIsCheckedAgainstTheFile(@TempDir Path tempDir) throws Exception {
        Path driver = driverFileContaining(tempDir, "abc");
        Assertions.assertDoesNotThrow(() -> AdbcDriverPathResolver.checkChecksum(
                driver, ABC_MD5, "libadbc_driver_flightsql.so"));
    }

    @Test
    void checksumIsComparedWithoutRegardToCase(@TempDir Path tempDir) throws Exception {
        Path driver = driverFileContaining(tempDir, "abc");
        // md5sum prints lowercase, other tools print upper; rejecting one of them would only teach
        // users that the property is unreliable.
        Assertions.assertDoesNotThrow(() -> AdbcDriverPathResolver.checkChecksum(
                driver, ABC_MD5.toUpperCase(java.util.Locale.ROOT), "libadbc_driver_flightsql.so"));
    }

    @Test
    void theWrongFileIsNamedAlongWithBothChecksums(@TempDir Path tempDir) throws Exception {
        Path driver = driverFileContaining(tempDir, "not the driver you meant");

        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> AdbcDriverPathResolver.checkChecksum(driver, ABC_MD5, "libadbc_driver_flightsql.so"));

        // Both values, because the useful next step is comparing them with the file the user meant to
        // deploy, and the path, because on this catalog type the usual mistake is a stale copy.
        String message = e.getMessage();
        Assertions.assertTrue(message.contains(ABC_MD5), message);
        Assertions.assertTrue(message.contains(driver.toString()), message);
        Assertions.assertTrue(message.contains(AdbcConnectorProperties.DRIVER_CHECKSUM), message);
    }

    @Test
    void noChecksumMeansNoCheck(@TempDir Path tempDir) throws Exception {
        Path driver = driverFileContaining(tempDir, "abc");
        for (String absent : new String[] {null, "", "   "}) {
            Assertions.assertDoesNotThrow(() -> AdbcDriverPathResolver.checkChecksum(
                    driver, absent, "libadbc_driver_flightsql.so"), String.valueOf(absent));
        }
    }
}
