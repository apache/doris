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

import org.junit.jupiter.api.Assumptions;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Locates the ADBC native libraries thirdparty builds, so the connector can be exercised against a real
 * driver instead of a stand-in.
 *
 * <p>The SQLite driver is not the source this connector targets, but it travels the identical path:
 * Java -> JNI bridge -> C driver manager -> a driver {@code .so}. That path is the whole reason FE uses the
 * JNI driver at all, so a test that stubs it out would leave the one risky part unexercised. What SQLite
 * cannot cover (it answers {@code NOT_IMPLEMENTED} to {@code executePartitioned} and {@code executeSchema})
 * is exactly the set of driver capability gaps the connector must degrade around, so it doubles as the
 * negative sample for those.
 *
 * <p>When the libraries are absent the tests SKIP, and say so loudly: a skipped run has verified nothing
 * about the native path and must not be read as a pass.
 */
final class AdbcNativeTestSupport {

    static final String JNI_LIBRARY_PATH_PROPERTY = "arrow.adbc.driver.jni.library.path";

    private static final String JNI_LIBRARY = "libadbc_driver_jni.so";
    private static final String SQLITE_DRIVER = "libadbc_driver_sqlite.so";

    private AdbcNativeTestSupport() {
    }

    /**
     * Returns the directory holding the native libraries, or skips the calling test.
     *
     * <p>Note the JNI property names a DIRECTORY, not a file: the resolver inside adbc-driver-jni appends
     * {@code System.mapLibraryName("adbc_driver_jni")} to it. Pointing it at the {@code .so} itself yields a
     * path like {@code .../libadbc_driver_jni.so/libadbc_driver_jni.so} and a load failure.
     */
    static Path requireNativeLibraryDir() {
        Path dir = findNativeLibraryDir();
        Assumptions.assumeTrue(dir != null,
                "SKIPPED: " + JNI_LIBRARY + " / " + SQLITE_DRIVER + " were not found. Build them with"
                        + " 'cd thirdparty && ./build-thirdparty.sh arrow_adbc', or set DORIS_THIRDPARTY."
                        + " THE ADBC NATIVE PATH IS NOT BEING EXERCISED BY THIS RUN.");
        System.setProperty(JNI_LIBRARY_PATH_PROPERTY, dir.toString());
        return dir;
    }

    static Path sqliteDriver() {
        return requireNativeLibraryDir().resolve(SQLITE_DRIVER);
    }

    private static Path findNativeLibraryDir() {
        String fromEnv = System.getenv("DORIS_THIRDPARTY");
        if (fromEnv != null && !fromEnv.isEmpty()) {
            Path candidate = Paths.get(fromEnv, "installed", "lib64");
            if (hasBothLibraries(candidate)) {
                return candidate;
            }
        }
        // Walk up from the module directory to the repository root, which is where thirdparty sits after a
        // local './build-thirdparty.sh' run.
        Path here = Paths.get("").toAbsolutePath();
        while (here != null) {
            Path candidate = here.resolve("thirdparty").resolve("installed").resolve("lib64");
            if (hasBothLibraries(candidate)) {
                return candidate;
            }
            here = here.getParent();
        }
        return null;
    }

    private static boolean hasBothLibraries(Path dir) {
        return Files.isReadable(dir.resolve(JNI_LIBRARY)) && Files.isReadable(dir.resolve(SQLITE_DRIVER));
    }
}
