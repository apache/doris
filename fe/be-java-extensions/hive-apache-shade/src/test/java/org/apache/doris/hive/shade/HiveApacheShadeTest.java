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

package org.apache.doris.hive.shade;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * This module exists to decide one thing - which copy of Hive's storage API a reader gets - and it
 * decides it by unpacking one jar over the other. That is an ordering, and an ordering is exactly
 * the kind of thing that goes wrong quietly: reverse the two executions, or bump a version so the
 * overlap moves, and the build still succeeds while a Hudi table on ORC starts failing at
 * {@code VectorizedRowBatch.setFilterContext}, which only the standalone artifact has.
 *
 * <p>So the outcome is asserted rather than trusted, against the two source jars themselves - which
 * means these tests keep meaning the same thing after either version changes, with no list of class
 * names here to go stale.
 */
public class HiveApacheShadeTest {

    private final Path merged = Paths.get(required("merged.classes"));
    private final Path repackagedHive = sourceJar("hive-apache-");
    private final Path standaloneStorageApi = sourceJar("hive-storage-api-");

    /** Every class the standalone artifact publishes is the copy that ends up in the jar. */
    @Test
    public void theStandaloneStorageApiWinsEveryClassItPublishes() throws IOException {
        Map<String, Long> storageApi = classesIn(standaloneStorageApi);
        Assert.assertFalse("the storage API jar was not found at " + standaloneStorageApi,
                storageApi.isEmpty());
        List<String> wrong = new ArrayList<>();
        for (Map.Entry<String, Long> clazz : storageApi.entrySet()) {
            Long actual = checksumOfMerged(clazz.getKey());
            if (!clazz.getValue().equals(actual)) {
                wrong.add(clazz.getKey() + (actual == null ? " (missing)" : " (other copy)"));
            }
        }
        Assert.assertEquals("classes not taken from hive-storage-api: " + wrong,
                0, wrong.size());
    }

    /**
     * And nothing of the repackaged hive is lost on the way: only the classes it shares with the
     * standalone artifact may differ, everything else - the serdes and object inspectors this is
     * bundled for in the first place - has to arrive byte for byte.
     */
    @Test
    public void everythingOnlyTheRepackagedHiveHasSurvivesUnchanged() throws IOException {
        Map<String, Long> hive = classesIn(repackagedHive);
        Map<String, Long> storageApi = classesIn(standaloneStorageApi);
        Assert.assertFalse("the repackaged hive jar was not found at " + repackagedHive,
                hive.isEmpty());
        List<String> wrong = new ArrayList<>();
        for (Map.Entry<String, Long> clazz : hive.entrySet()) {
            if (storageApi.containsKey(clazz.getKey())) {
                continue;
            }
            Long actual = checksumOfMerged(clazz.getKey());
            if (!clazz.getValue().equals(actual)) {
                wrong.add(clazz.getKey() + (actual == null ? " (missing)" : " (altered)"));
            }
        }
        Assert.assertEquals("classes lost or altered from hive-apache: " + wrong, 0, wrong.size());
    }

    /**
     * The overlap is the whole reason this module exists, so it must not silently become empty: a
     * version pair that no longer collides means the merge is doing nothing and the two jars could
     * go back to being separate dependencies.
     */
    @Test
    public void thereIsAnOverlapToResolve() throws IOException {
        Map<String, Long> hive = classesIn(repackagedHive);
        int overlap = 0;
        for (String clazz : classesIn(standaloneStorageApi).keySet()) {
            if (hive.containsKey(clazz)) {
                overlap++;
            }
        }
        Assert.assertTrue("the two jars no longer share classes; this module has nothing to do",
                overlap > 0);
    }

    private Long checksumOfMerged(String entry) throws IOException {
        Path file = merged.resolve(entry);
        if (!Files.isRegularFile(file)) {
            return null;
        }
        CRC32 crc = new CRC32();
        crc.update(Files.readAllBytes(file));
        return crc.getValue();
    }

    /** Entry name to CRC32 of its contents, which a zip stores and a file has to be read for. */
    private static Map<String, Long> classesIn(Path jar) throws IOException {
        Map<String, Long> classes = new HashMap<>();
        try (ZipFile zip = new ZipFile(jar.toFile())) {
            for (ZipEntry entry : java.util.Collections.list(zip.entries())) {
                if (entry.getName().endsWith(".class")) {
                    classes.put(entry.getName(), entry.getCrc());
                }
            }
        }
        return classes;
    }

    /**
     * The source jars as the build put them beside the merged tree, found by name rather than by
     * version so that bumping one is a one-line change in the pom.
     */
    private static Path sourceJar(String prefix) {
        Path dir = Paths.get(required("source.jars"));
        try (java.util.stream.Stream<Path> files = Files.list(dir)) {
            for (Path file : (Iterable<Path>) files::iterator) {
                String name = file.getFileName().toString();
                if (name.startsWith(prefix) && name.endsWith(".jar")) {
                    return file;
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("cannot list " + dir, e);
        }
        throw new IllegalStateException("no " + prefix + "*.jar in " + dir);
    }

    private static String required(String property) {
        String value = System.getProperty(property);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException("system property " + property + " is not set; the pom "
                    + "passes it to surefire, see the dependency plugin's properties goal");
        }
        return value;
    }
}
