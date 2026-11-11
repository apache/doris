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

import org.apache.doris.common.io.Text;
import org.apache.doris.datasource.Legacy413Fixtures.LegacyLabel;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Access to the golden Doris 4.1.5 metadata fixtures under {@code src/test/resources/upgrade/415/}: the Lance
 * catalogs 4.1.4 introduced, in the bytes a real 4.1.5 FE writes (see the PROVENANCE.txt next to them).
 */
public final class Legacy415Fixtures {

    /** The lastUpdateTime the generator stamps on every catalog, so the bytes are deterministic. */
    public static final long FIXED_UPDATE_TIME = 1753000000000L;

    private static final String ROOT = "/upgrade/415/";

    private Legacy415Fixtures() {
    }

    /** The image's "datasource" module as a 4.1.5 FE would have written it. */
    public static CatalogMgr loadCatalogMgr() throws IOException {
        return Legacy413Fixtures.loadCatalogMgr(ROOT, "datasource.module.bin");
    }

    /** The same module's JSON payload, unframed, for assertions on what is literally on the wire. */
    public static String catalogMgrJson() throws IOException {
        byte[] framed = Legacy413Fixtures.resource(ROOT, "datasource.module.bin");
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(framed))) {
            return Text.readString(in);
        }
    }

    /** Raw bytes of one edit-log entry, in JournalEntity wire form (opCode short + Text-framed JSON). */
    public static byte[] journalEntry(String fileName) throws IOException {
        return Legacy413Fixtures.resource(ROOT, "editlog/" + fileName);
    }

    /** Names of every edit-log fixture with the given op code, e.g. {@code 320}. */
    public static List<String> journalEntryNames(int opCode) {
        return Legacy413Fixtures.journalEntryNames(ROOT, opCode);
    }

    /**
     * One of 4.1.5's own GSON label registries, read out of the generated {@code labels.*.txt}.
     *
     * @param which one of {@code ds}, {@code db}, {@code tbl}
     */
    public static Map<String, LegacyLabel> legacyLabels(String which) throws IOException {
        return Legacy413Fixtures.legacyLabels(ROOT, which);
    }
}
