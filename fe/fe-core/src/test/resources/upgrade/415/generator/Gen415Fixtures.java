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

package org.apache.doris.tools;

import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Text;
import org.apache.doris.datasource.CatalogFactory;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogLog;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.persist.gson.RuntimeTypeAdapterFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Generates golden metadata fixtures EXACTLY as Doris 4.1.5 writes them, for the Lance catalogs that
 * 4.1.4 introduced.
 *
 * <p>This class is NOT part of Doris. It runs only against a throwaway tree checked out at the 4.1.5-release
 * tag, so that the fixtures it emits are produced by real 4.1.5 bytecode rather than by someone's idea of
 * what 4.1.5 writes. The 4.1.3 fixtures next to these (upgrade/413) were made the same way.</p>
 *
 * <p>Usage: {@code java -cp <fe-core classes + deps> org.apache.doris.tools.Gen415Fixtures <outDir>}</p>
 */
public class Gen415Fixtures {

    /** Fixed so the output is byte-deterministic across runs; the same value the 4.1.3 generator stamps. */
    private static final long FIXED_UPDATE_TIME = 1753000000000L;

    private final List<CatalogIf<?>> catalogs = new ArrayList<>();
    private final List<String> failures = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("usage: Gen415Fixtures <outDir>");
        }
        FeConstants.runningUnitTest = true;
        new Gen415Fixtures().run(new File(args[0]));
    }

    private void run(File outDir) throws Exception {
        if (!outDir.exists() && !outDir.mkdirs()) {
            throw new IllegalStateException("cannot create " + outDir);
        }
        buildLance();

        CatalogMgr mgr = newCatalogMgrWith(catalogs);
        String json = GsonUtils.GSON.toJson(mgr);

        writeFramed(new File(outDir, "datasource.module.bin"), json);
        Files.write(Paths.get(new File(outDir, "catalogMgr.json").toURI()), json.getBytes(StandardCharsets.UTF_8));
        dumpLabels(outDir);
        buildEditLogFixtures(new File(outDir, "editlog"));

        System.out.println("=== catalogs written: " + catalogs.size());
        for (CatalogIf<?> c : catalogs) {
            System.out.println("    " + c.getId() + "  " + c.getClass().getSimpleName() + "  " + c.getName());
        }
        System.out.println("=== module sha256: " + sha256(json.getBytes(StandardCharsets.UTF_8)));
        if (!failures.isEmpty()) {
            System.out.println("=== FAILURES (" + failures.size() + "):");
            failures.forEach(f -> System.out.println("    " + f));
            throw new IllegalStateException(failures.size() + " fixture(s) could not be built");
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // Lance catalogs, built through 4.1.5's own CatalogFactory replay path, i.e. what a catalog CREATED ON
    // 4.1.4+ looks like. Their props always carry "type": Resource.ResourceType has no LANCE value, so a
    // resource-backed Lance catalog cannot exist and there is no 4.1.3-style G2 family to add here.
    // ---------------------------------------------------------------------------------------------------
    private void buildLance() {
        // The shape of the external_table_p0/lance suites: a filesystem namespace on S3-compatible storage.
        lance(10101, "g1_lance_s3", "lance on s3", props("type", "lance",
                "lance.catalog.type", "filesystem", "warehouse", "s3://warehouse/lance",
                "s3.endpoint", "http://minio:9000", "s3.access_key", "ak", "s3.secret_key", "sk",
                "s3.region", "us-east-1", "use_path_style", "true"), true);
        lance(10102, "g1_lance_local", "", props("type", "lance",
                "lance.catalog.type", "filesystem", "warehouse", "file:///tmp/lance_wh"), false);
        lance(10103, "g1_lance_rest", "", props("type", "lance",
                "lance.catalog.type", "rest", "lance.rest.uri", "http://lance-rest:2333",
                "lance.rest.security.type", "bearer", "lance.rest.bearer-token", "tok",
                "lance.namespace.delimiter", ".", "lance.namespace.root_database", "default"), false);
    }

    private void lance(long id, String name, String comment, Map<String, String> props, boolean withTaap) {
        try {
            CatalogLog log = new CatalogLog();
            log.setCatalogId(id);
            log.setCatalogName(name);
            log.setComment(comment);
            log.setProps(props);
            CatalogIf<?> c = CatalogFactory.createFromLog(log);
            decorate(c, withTaap);
            catalogs.add(c);
        } catch (Throwable t) {
            failures.add(name + ": " + t);
        }
    }

    private void decorate(CatalogIf<?> c, boolean withTaap) {
        ExternalCatalog ec = (ExternalCatalog) c;
        ec.setLastUpdateTime(FIXED_UPDATE_TIME);
        if (withTaap) {
            // Empty map serializes as {}, non-empty as an array of [key, value] 2-tuples
            // (enableComplexMapKeySerialization); keep one catalog on each encoding.
            ec.setAutoAnalyzePolicy("db1", "tbl1", "enable");
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // Edit-log fixtures: JournalEntity wire form is writeShort(opCode) + payload.write(out), and
    // CatalogLog.write is Text.writeString(out, GSON.toJson(this)). OP_CREATE_CATALOG only; the other
    // catalog ops carry no catalog type and are covered by the 4.1.3 fixtures.
    // ---------------------------------------------------------------------------------------------------
    private void buildEditLogFixtures(File dir) throws Exception {
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IllegalStateException("cannot create " + dir);
        }
        for (CatalogIf<?> c : catalogs) {
            writeOp(dir, 320, "create-" + c.getName(), c.constructEditLog());
        }

        String[] names = dir.list();
        if (names == null) {
            throw new IllegalStateException("cannot list " + dir);
        }
        java.util.Arrays.sort(names);
        StringBuilder sb = new StringBuilder("# generated by Gen415Fixtures -- do not edit by hand\n");
        for (String n : names) {
            sb.append(n).append('\n');
        }
        Files.write(Paths.get(new File(dir.getParentFile(), "editlog.index").toURI()),
                sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private void writeOp(File dir, int opCode, String name, CatalogLog log) throws Exception {
        File f = new File(dir, String.format("op%d-%s.bin", opCode, name));
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(f))) {
            out.writeShort(opCode);
            log.write(out);
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // Label inventory, read reflectively out of 4.1.5's own registries (a regex scrape drops labels whose
    // class name contains a digit).
    // ---------------------------------------------------------------------------------------------------
    private void dumpLabels(File outDir) throws Exception {
        dumpOneRegistry(outDir, "labels.ds.txt", "dsTypeAdapterFactory");
        dumpOneRegistry(outDir, "labels.db.txt", "dbTypeAdapterFactory");
        dumpOneRegistry(outDir, "labels.tbl.txt", "tblTypeAdapterFactory");
    }

    private void dumpOneRegistry(File outDir, String fileName, String fieldName) throws Exception {
        Field f = GsonUtils.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        RuntimeTypeAdapterFactory<?> factory = (RuntimeTypeAdapterFactory<?>) f.get(null);
        Field l = RuntimeTypeAdapterFactory.class.getDeclaredField("labelToSubtype");
        l.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Class<?>> labelToSubtype = (Map<String, Class<?>>) l.get(factory);
        StringBuilder sb = new StringBuilder();
        sb.append("# generated from 4.1.5 by Gen415Fixtures -- do not edit by hand\n");
        sb.append("# <label>\\t<target class FQN>\\t<ABSTRACT if the label can never be emitted>\n");
        new TreeMap<>(labelToSubtype).forEach((label, clazz) -> sb.append(label).append('\t')
                .append(clazz.getName())
                .append(java.lang.reflect.Modifier.isAbstract(clazz.getModifiers()) ? "\tABSTRACT" : "")
                .append('\n'));
        Files.write(Paths.get(new File(outDir, fileName).toURI()), sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    // ---------------------------------------------------------------------------------------------------
    // helpers (same as the 4.1.3 generator)
    // ---------------------------------------------------------------------------------------------------

    /**
     * A CatalogMgr holding the internal catalog (added by its own ctor) plus the given externals, injected
     * straight into idToCatalog: addCatalog() needs a live Env. Only idToCatalog is @SerializedName.
     */
    private CatalogMgr newCatalogMgrWith(List<CatalogIf<?>> externals) throws Exception {
        CatalogMgr mgr = new CatalogMgr();
        Field f = CatalogMgr.class.getDeclaredField("idToCatalog");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<Long, CatalogIf<?>> idToCatalog = (ConcurrentMap<Long, CatalogIf<?>>) f.get(mgr);
        for (CatalogIf<?> c : externals) {
            idToCatalog.put(c.getId(), c);
        }
        return mgr;
    }

    private static void writeFramed(File file, String json) throws Exception {
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(file))) {
            Text.writeString(out, json);
        }
    }

    private static Map<String, String> props(String... kv) {
        if (kv.length % 2 != 0) {
            throw new IllegalArgumentException("odd number of props");
        }
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static String sha256(byte[] bytes) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        StringBuilder sb = new StringBuilder();
        for (byte b : md.digest(bytes)) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
