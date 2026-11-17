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

package org.apache.doris.connector.lance;

import org.junit.jupiter.api.Assumptions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;

/**
 * What a test needs to read a real Lance dataset: the native libraries and a committed dataset.
 *
 * <p>Two native libraries are involved: Lance JNI opens datasets and Arrow C Data JNI imports their
 * schemas. Linux deployment replaces both upstream libraries for older hosts; Maven tests use the
 * upstream jars and skip when either library cannot load. A skipped run has verified nothing about
 * the native path and must not be read as a pass. The regression suites under
 * {@code regression-test/suites/external_table_p0/lance} cover the path on every pipeline.
 *
 * <p>The datasets are the ones the BE Lance reader tests and the regression environment already read, so FE
 * and BE are checked against the same bytes.
 */
final class LanceNativeTestSupport {

    /** Three versions of one table: what time travel needs. Shared with the BE Lance reader tests. */
    static final String VERSIONED_ALL_TYPES = "be/test/format_v2/table/lance/data/all_types.lance";

    private LanceNativeTestSupport() {
    }

    /** Loads both native libraries, or skips the calling test. */
    static void requireNativeLibraries() {
        try {
            org.lance.JniLoader.ensureLoaded();
            org.apache.arrow.c.jni.JniLoader.get().ensureLoaded();
        } catch (Throwable e) {
            Assumptions.abort("SKIPPED: the Lance or Arrow C Data JNI library cannot be loaded on this host ("
                    + e + "). THE LANCE NATIVE PATH IS NOT BEING EXERCISED BY THIS RUN.");
        }
    }

    /** Copies a committed dataset into {@code warehouse} under its own directory name, or skips. */
    static Path copyDataset(String repositoryRelativePath, Path warehouse) throws IOException {
        Path source = findInRepository(repositoryRelativePath);
        Assumptions.assumeTrue(source != null, "SKIPPED: " + repositoryRelativePath
                + " was not found above " + Paths.get("").toAbsolutePath()
                + ". THE LANCE NATIVE PATH IS NOT BEING EXERCISED BY THIS RUN.");
        Path target = warehouse.resolve(source.getFileName().toString());
        try (Stream<Path> files = Files.walk(source)) {
            for (Path file : (Iterable<Path>) files::iterator) {
                Path copy = target.resolve(source.relativize(file).toString());
                if (Files.isDirectory(file)) {
                    Files.createDirectories(copy);
                } else {
                    Files.copy(file, copy, StandardCopyOption.COPY_ATTRIBUTES);
                }
            }
        }
        return target;
    }

    /** Walks up from the module directory to the repository root, where the dataset sits. */
    private static Path findInRepository(String repositoryRelativePath) {
        Path here = Paths.get("").toAbsolutePath();
        while (here != null) {
            Path candidate = here.resolve(repositoryRelativePath);
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
            here = here.getParent();
        }
        return null;
    }
}
