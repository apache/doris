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

import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Text;
import org.apache.doris.datasource.CatalogFactory;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogLog;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.es.EsExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.jdbc.JdbcExternalCatalog;
import org.apache.doris.datasource.lakesoul.LakeSoulExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonDLFExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonFileExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonHMSExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonRestExternalCatalog;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalCatalog;
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
 * Generates golden metadata fixtures EXACTLY as Doris 4.1.3 writes them.
 *
 * <p>This class is NOT part of Doris. It lives only in a throwaway git worktree checked out at the
 * 4.1.3 tag, so that the fixtures it emits are produced by real 4.1.3 bytecode rather than by
 * someone's idea of what 4.1.3 writes. See plan-doc/41-upgrade-compat-test-plan.md.</p>
 *
 * <p>Usage: {@code java -cp <fe-core classes + deps> org.apache.doris.tools.Gen413Fixtures <outDir> [cloud]}</p>
 */
public class Gen413Fixtures {

    /** Fixed so the output is byte-deterministic across runs. */
    private static final long FIXED_UPDATE_TIME = 1753000000000L;

    private final List<CatalogIf<?>> catalogs = new ArrayList<>();
    private final List<String> failures = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("usage: Gen413Fixtures <outDir> [cloud]");
        }
        File outDir = new File(args[0]);
        boolean cloud = args.length > 1 && "cloud".equals(args[1]);
        // MUST happen before GsonUtils / EnvFactory class-init: both latch the mode in static initializers.
        if (cloud) {
            Config.deploy_mode = "cloud";
        }
        FeConstants.runningUnitTest = true;
        new Gen413Fixtures().run(outDir, cloud);
    }

    private void run(File outDir, boolean cloud) throws Exception {
        if (!outDir.exists() && !outDir.mkdirs()) {
            throw new IllegalStateException("cannot create " + outDir);
        }
        buildG1();
        buildG2();
        buildG3();

        CatalogMgr mgr = newCatalogMgrWith(catalogs);
        String json = GsonUtils.GSON.toJson(mgr);

        String suffix = cloud ? ".cloud" : "";
        writeFramed(new File(outDir, "datasource.module" + suffix + ".bin"), json);
        Files.write(Paths.get(new File(outDir, "catalogMgr" + suffix + ".json").toURI()),
                json.getBytes(StandardCharsets.UTF_8));

        if (!cloud) {
            dumpLabels(outDir);
            buildEditLogFixtures(new File(outDir, "editlog"));
        }

        System.out.println("=== catalogs written: " + catalogs.size() + " (cloud=" + cloud + ")");
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
    // G1 -- built through 4.1.3's own CatalogFactory replay path, props DO carry "type".
    // This is what a catalog created ON 4.1.3 looks like.
    // ---------------------------------------------------------------------------------------------------
    private void buildG1() {
        g1(10001, "g1_hms", "my hive", props("type", "hms",
                "hive.metastore.uris", "thrift://hms-host:9083"), true);
        g1(10002, "g1_ice_hms", "", props("type", "iceberg",
                "iceberg.catalog.type", "hms", "hive.metastore.uris", "thrift://hms-host:9083"), false);
        g1(10003, "g1_ice_rest", "", props("type", "iceberg",
                "iceberg.catalog.type", "rest", "uri", "http://rest:8181"), false);
        g1(10004, "g1_ice_glue", "", props("type", "iceberg",
                "iceberg.catalog.type", "glue", "glue.region", "us-east-1", "warehouse", "s3://b/w"), false);
        g1(10005, "g1_ice_dlf", "", props("type", "iceberg",
                "iceberg.catalog.type", "dlf", "dlf.region", "cn-beijing"), false);
        g1(10006, "g1_ice_hadoop", "", props("type", "iceberg",
                "iceberg.catalog.type", "hadoop", "warehouse", "hdfs://nn:8020/wh"), false);
        g1(10007, "g1_ice_jdbc", "", props("type", "iceberg",
                "iceberg.catalog.type", "jdbc", "uri", "jdbc:postgresql://pg:5432/ice", "warehouse", "s3://b/w"), false);
        g1(10008, "g1_ice_s3tables", "", props("type", "iceberg",
                "iceberg.catalog.type", "s3tables", "warehouse", "arn:aws:s3tables:us-east-1:1:bucket/b"), false);
        g1(10009, "g1_paimon", "paimon fs", props("type", "paimon",
                "warehouse", "file:/tmp/paimon_wh"), false);
        g1(10010, "g1_es", "", props("type", "es", "hosts", "http://es:9200"), false);
        // jdbc_url deliberately carries '=' and '&': they are HTML-escaped on the wire (= / &).
        g1(10011, "g1_jdbc", "", props("type", "jdbc",
                "user", "root", "password", "pwd",
                "jdbc_url", "jdbc:mysql://mysql:3306/db?useSSL=false&serverTimezone=UTC",
                "driver_url", "mysql-connector-j-8.3.0.jar",
                "driver_class", "com.mysql.cj.jdbc.Driver"), false);
        g1(10012, "g1_maxcompute", "", props("type", "max_compute",
                "mc.default.project", "p", "mc.access_key", "ak", "mc.secret_key", "sk",
                "mc.endpoint", "http://service.cn.maxcompute.aliyun.com/api"), false);
        g1(10013, "g1_trino", "", props("type", "trino-connector",
                "trino.connector.name", "tpch"), false);
        // Control group: the engine implements this type itself, so after the SPI cutover it must KEEP
        // its own class and must NOT be remapped onto PluginDrivenExternalCatalog.
        // ("test" is deliberately absent: TestExternalCatalog needs a UT-only provider class and can
        // never appear in a production 4.1.3 image.)
        g1(10015, "g1_remote_doris", "", props("type", "doris",
                "jdbc.jdbc_url", "jdbc:mysql://doris-fe:9030/db",
                "jdbc.user", "root", "jdbc.password", "",
                "jdbc.driver_url", "mysql-connector-j-8.3.0.jar",
                "jdbc.driver_class", "com.mysql.cj.jdbc.Driver"), false);
    }

    private void g1(long id, String name, String comment, Map<String, String> props, boolean withTaap) {
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
            failures.add("G1 " + name + ": " + t);
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // G2 -- resource-backed shape: props carry NO "type" key, catalogProperty.resource is set.
    // 4.1.3 did not need "type" because the concrete class carried it; after the SPI cutover the type
    // can only be recovered from logType. This family is the ONLY one that can fail when HEAD's
    // PluginDrivenExternalCatalog.gsonPostProcess backfill is removed.
    // Built by calling the concrete ctor + setDefaultPropsIfMissing(true), which is literally what
    // 4.1.3's CatalogFactory switch does after picking the class.
    // ---------------------------------------------------------------------------------------------------
    private void buildG2() {
        g2(10020, "g2_hms_res", () -> new HMSExternalCatalog(10020, "g2_hms_res", "hms_res",
                props("hive.metastore.uris", "thrift://hms-host:9083"), "resource backed hms"));
        g2(10021, "g2_es_res", () -> new EsExternalCatalog(10021, "g2_es_res", "es_res",
                props("hosts", "http://es:9200"), ""));
        g2(10022, "g2_jdbc_res", () -> new JdbcExternalCatalog(10022, "g2_jdbc_res", "jdbc_res",
                props("user", "root", "password", "pwd",
                        "jdbc_url", "jdbc:mysql://mysql:3306/db?useSSL=false&serverTimezone=UTC",
                        "driver_url", "mysql-connector-j-8.3.0.jar",
                        "driver_class", "com.mysql.cj.jdbc.Driver"), ""));
        // The load-bearing one: TRINO_CONNECTOR is the only logType whose name().toLowerCase()
        // ("trino_connector") differs from the catalog type string the connector answers to
        // ("trino-connector").
        g2(10023, "g2_trino_res", () -> new TrinoConnectorExternalCatalog(10023, "g2_trino_res", "trino_res",
                props("trino.connector.name", "tpch"), ""));
    }

    // ---------------------------------------------------------------------------------------------------
    // G3 -- labels that exist in 4.1.3's GsonUtils and can appear in a 4.1 image, but that 4.1.3's own
    // factories can no longer produce (the paimon factory always returns the base PaimonExternalCatalog;
    // lakesoul creation was already rejected). They are carry-forward from <= 4.0 metadata.
    // Still real 4.1.3 bytecode -- only the construction route differs. Recorded in PROVENANCE.txt.
    // ---------------------------------------------------------------------------------------------------
    private void buildG3() {
        g2(10030, "g3_paimon_hms", () -> new PaimonHMSExternalCatalog(10030, "g3_paimon_hms", "",
                props("type", "paimon", "paimon.catalog.type", "hms",
                        "hive.metastore.uris", "thrift://hms-host:9083", "warehouse", "hdfs://nn:8020/wh"), ""));
        g2(10031, "g3_paimon_file", () -> new PaimonFileExternalCatalog(10031, "g3_paimon_file", "",
                props("type", "paimon", "paimon.catalog.type", "filesystem",
                        "warehouse", "file:/tmp/paimon_wh"), ""));
        g2(10032, "g3_paimon_rest", () -> new PaimonRestExternalCatalog(10032, "g3_paimon_rest", "",
                props("type", "paimon", "paimon.catalog.type", "rest", "uri", "http://paimon-rest:8080"), ""));
        g2(10033, "g3_paimon_dlf", () -> new PaimonDLFExternalCatalog(10033, "g3_paimon_dlf", "",
                props("type", "paimon", "paimon.catalog.type", "dlf", "warehouse", "oss://b/w"), ""));
        g2(10034, "g3_lakesoul", () -> new LakeSoulExternalCatalog(10034, "g3_lakesoul", "",
                props("type", "lakesoul", "lakesoul.pg.url", "jdbc:postgresql://pg:5432/lakesoul"), ""));
    }

    private interface CatalogSupplier {
        ExternalCatalog get() throws Exception;
    }

    private void g2(long id, String name, CatalogSupplier supplier) {
        try {
            ExternalCatalog c = supplier.get();
            // exactly what 4.1.3's CatalogFactory does right after picking the concrete class
            c.setDefaultPropsIfMissing(true);
            decorate(c, false);
            catalogs.add(c);
        } catch (Throwable t) {
            failures.add("G2/G3 " + name + " (id=" + id + "): " + t);
        }
    }

    private void decorate(CatalogIf<?> c, boolean withTaap) {
        if (!(c instanceof ExternalCatalog)) {
            return;
        }
        ExternalCatalog ec = (ExternalCatalog) c;
        ec.setLastUpdateTime(FIXED_UPDATE_TIME);
        if (withTaap) {
            // Exercises the polymorphic taap encoding: empty map serializes as {}, non-empty as
            // an array of [key, value] 2-tuples (enableComplexMapKeySerialization).
            ec.setAutoAnalyzePolicy("db1", "tbl1", "enable");
            ec.setAutoAnalyzePolicy("db2", "tbl2", "disable");
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // Edit-log fixtures: JournalEntity wire form is writeShort(opCode) + payload.write(out),
    // and CatalogLog.write is Text.writeString(out, GSON.toJson(this)). Nothing about the class is
    // on the wire, so a 4.1.3 entry and a HEAD entry are byte-identical -- only replay behaviour differs.
    // ---------------------------------------------------------------------------------------------------
    private void buildEditLogFixtures(File dir) throws Exception {
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IllegalStateException("cannot create " + dir);
        }
        // OP_CREATE_CATALOG built from the real catalogs via CatalogIf.constructEditLog(),
        // i.e. exactly what 4.1.3's CatalogMgr.createCatalog would have logged.
        //
        // G1 ONLY, on purpose. constructEditLog() hardcodes setResource("") and journals nothing but the
        // property map, so a resource-backed catalog (G2) would produce an entry carrying neither a resource
        // nor a "type" -- a shape 4.1.3 can never emit, because 4.1.3 has no CREATE CATALOG ... WITH RESOURCE
        // and therefore never journals such a catalog in the first place. G2 reaches an upgraded FE through
        // the image, never through the journal. G3 is likewise unjournalable (the paimon factory always
        // returns the base class, lakesoul creation is rejected outright) and its props are indistinguishable
        // from G1's on the wire anyway, since the class name is not part of a journal entry.
        for (CatalogIf<?> c : catalogs) {
            if (!(c instanceof ExternalCatalog) || !c.getName().startsWith("g1_")) {
                continue;
            }
            writeOp(dir, 320, "create-" + c.getName(), c.constructEditLog());
        }
        CatalogLog drop = new CatalogLog();
        drop.setCatalogId(10001);
        writeOp(dir, 321, "drop", drop);

        CatalogLog rename = new CatalogLog();
        rename.setCatalogId(10001);
        rename.setNewCatalogName("g1_hms_renamed");
        writeOp(dir, 322, "alter-name", rename);

        CatalogLog alterProps = new CatalogLog();
        alterProps.setCatalogId(10001);
        alterProps.setNewProps(props("hive.metastore.uris", "thrift://other-hms:9083",
                "metadata_refresh_interval_sec", "30"));
        writeOp(dir, 323, "alter-props", alterProps);

        CatalogLog refresh = new CatalogLog();
        refresh.setCatalogId(10001);
        refresh.setInvalidCache(true);
        writeOp(dir, 324, "refresh", refresh);

        CatalogLog comment = new CatalogLog();
        comment.setCatalogId(10001);
        comment.setComment("new comment");
        writeOp(dir, 458, "alter-comment", comment);

        // Flat index: resource directories cannot be enumerated from a jar, so the tests read this
        // instead of listing the directory. Regenerating the fixtures regenerates the index, which is
        // why it lives here rather than being maintained by hand.
        String[] names = dir.list();
        if (names == null) {
            throw new IllegalStateException("cannot list " + dir);
        }
        java.util.Arrays.sort(names);
        StringBuilder sb = new StringBuilder("# generated by Gen413Fixtures -- do not edit by hand\n");
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
    // Label inventory, read reflectively out of 4.1.3's own registry. Machine-generated on purpose:
    // a hand-written or regex-scraped list silently drops entries (e.g. IcebergS3TablesExternalCatalog,
    // whose class name contains a digit).
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
        sb.append("# generated from 4.1.3 by Gen413Fixtures -- do not edit by hand\n");
        sb.append("# <label>\\t<target class FQN>\\t<ABSTRACT if the label can never be emitted>\n");
        new TreeMap<>(labelToSubtype).forEach((label, clazz) -> sb.append(label).append('\t')
                .append(clazz.getName())
                .append(java.lang.reflect.Modifier.isAbstract(clazz.getModifiers()) ? "\tABSTRACT" : "")
                .append('\n'));
        Files.write(Paths.get(new File(outDir, fileName).toURI()), sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    // ---------------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------------

    /**
     * A CatalogMgr holding the internal catalog (added by its own ctor) plus the given externals.
     * The externals are injected straight into idToCatalog rather than through addCatalog(), because
     * addCatalog() calls resetToUninitialized() which needs a live Env we deliberately do not have.
     * Only idToCatalog is @SerializedName, so the emitted bytes are unaffected.
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
