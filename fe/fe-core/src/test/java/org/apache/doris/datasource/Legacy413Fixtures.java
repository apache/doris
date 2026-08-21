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

package org.apache.doris.datasource;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Access to the golden Doris 4.1.3 metadata fixtures under {@code src/test/resources/upgrade/413/}.
 *
 * <p>Those files were produced by real 4.1.3 bytecode, not written by hand; see the PROVENANCE.txt
 * next to them for how to regenerate and for the five wire-format details that make hand-authored
 * fixtures wrong.
 */
public final class Legacy413Fixtures {

    /** The lastUpdateTime the generator stamps on every catalog, so the bytes are deterministic. */
    public static final long FIXED_UPDATE_TIME = 1753000000000L;

    private static final String ROOT = "/upgrade/413/";

    private Legacy413Fixtures() {
    }

    /** The image's "datasource" module as a 4.1.3 FE would have written it. */
    public static CatalogMgr loadCatalogMgr() throws IOException {
        return loadCatalogMgr("datasource.module.bin");
    }

    /** Same, for the variant generated with {@code Config.deploy_mode="cloud"}. */
    public static CatalogMgr loadCloudCatalogMgr() throws IOException {
        return loadCatalogMgr("datasource.module.cloud.bin");
    }

    private static CatalogMgr loadCatalogMgr(String fileName) throws IOException {
        try (InputStream in = open(fileName);
                DataInputStream dis = new DataInputStream(in)) {
            return CatalogMgr.read(dis);
        }
    }

    /** Raw bytes of one edit-log entry, in JournalEntity wire form (opCode short + Text-framed JSON). */
    public static byte[] journalEntry(String fileName) throws IOException {
        try (InputStream in = open("editlog/" + fileName)) {
            return readAll(in);
        }
    }

    /** Names of every edit-log fixture with the given op code, e.g. {@code 320}. */
    public static List<String> journalEntryNames(int opCode) {
        List<String> names = new ArrayList<>();
        for (String name : listEditLogFixtures()) {
            if (name.startsWith("op" + opCode + "-")) {
                names.add(name);
            }
        }
        if (names.isEmpty()) {
            throw new IllegalStateException("no edit-log fixture for op " + opCode
                    + "; the resource index at " + ROOT + "editlog.index is stale");
        }
        return names;
    }

    private static List<String> listEditLogFixtures() {
        // Resource directories are not enumerable from a jar, and surefire may well run these classes off one.
        // The generator writes a flat index next to the fixtures precisely so this does not need directory
        // listing; keeping it in one place means a fixture added without regenerating the index fails loudly
        // in journalEntryNames() rather than silently reducing coverage.
        try (InputStream in = open("editlog.index")) {
            String text = new String(readAll(in), StandardCharsets.UTF_8);
            List<String> names = new ArrayList<>();
            for (String line : text.split("\n")) {
                String trimmed = line.trim();
                if (!trimmed.isEmpty() && !trimmed.startsWith("#")) {
                    names.add(trimmed);
                }
            }
            return names;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * One of 4.1.3's own GSON label registries, read out of the generated {@code labels.*.txt}.
     *
     * @param which one of {@code ds}, {@code db}, {@code tbl}
     * @return label to the fully-qualified name of the class 4.1.3 mapped it to, in file order
     */
    public static Map<String, LegacyLabel> legacyLabels(String which) throws IOException {
        Map<String, LegacyLabel> labels = new LinkedHashMap<>();
        try (InputStream in = open("labels." + which + ".txt")) {
            String text = new String(readAll(in), StandardCharsets.UTF_8);
            for (String line : text.split("\n")) {
                if (line.trim().isEmpty() || line.startsWith("#")) {
                    continue;
                }
                String[] parts = line.split("\t");
                boolean isAbstract = parts.length > 2 && "ABSTRACT".equals(parts[2].trim());
                labels.put(parts[0], new LegacyLabel(parts[0], parts[1].trim(), isAbstract));
            }
        }
        return labels;
    }

    /** One entry of a 4.1.3 GSON label registry. */
    public static final class LegacyLabel {
        public final String label;
        public final String legacyClassName;
        /**
         * Whether 4.1.3's class was abstract. GSON writes the runtime class's label, so an abstract
         * class's label can never actually appear in an image -- it is registered but unreachable.
         */
        public final boolean isAbstract;

        LegacyLabel(String label, String legacyClassName, boolean isAbstract) {
            this.label = label;
            this.legacyClassName = legacyClassName;
            this.isAbstract = isAbstract;
        }

        @Override
        public String toString() {
            return label + " -> " + legacyClassName + (isAbstract ? " (abstract)" : "");
        }
    }

    private static InputStream open(String relative) throws IOException {
        InputStream in = Legacy413Fixtures.class.getResourceAsStream(ROOT + relative);
        if (in == null) {
            throw new IOException("missing 4.1.3 fixture: " + ROOT + relative
                    + " -- see src/test/resources/upgrade/413/PROVENANCE.txt");
        }
        return in;
    }

    private static byte[] readAll(InputStream in) throws IOException {
        byte[] buffer = new byte[8192];
        int size = 0;
        int read;
        while ((read = in.read(buffer, size, buffer.length - size)) > 0) {
            size += read;
            if (size == buffer.length) {
                buffer = Arrays.copyOf(buffer, buffer.length * 2);
            }
        }
        return Arrays.copyOf(buffer, size);
    }
}
