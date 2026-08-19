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

package org.apache.doris.hudi;

import org.apache.doris.jni.spi.DorisPlugin;
import org.apache.doris.jni.spi.JniScanner;
import org.apache.doris.jni.spi.JniScannerFactory;
import org.apache.doris.jni.spi.utils.OffHeap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.filter.MutableFilterContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * BE addresses this plugin by name and never by class name, so none of that is checked at compile
 * time: a services file naming a class that moved, or a factory renamed, compiles and then fails as
 * "plugin hudi has no factory named reader" on the query that needed it. The names asserted here
 * are the deployment contract with the table in BE's jni_plugin_registry.h.
 *
 * <p>What this file does <em>not</em> check is whether the deployed plugin directory contains
 * everything a scan needs. Surefire puts {@code provided} dependencies on the test classpath, so
 * these tests would still pass with a dependency marked provided and therefore absent from the
 * plugin directory. Only loading the deployed directory through the plugin registry - see the
 * acceptance recipe in the migration notes - can catch that.
 */
public class HadoopHudiPluginTest {

    private static final String INSTANT = "20240101000000000";
    private static final String FILE_ID = "f0000000-0000-0000-0000-000000000000-0";

    /**
     * The columns FE sends, which for a Hudi table are the hive table's - so they start with the
     * five fields Hudi stamps into every record and the input format insists on finding.
     */
    private static final String[] COLUMNS = {
            "_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key",
            "_hoodie_partition_path", "_hoodie_file_name", "id", "name",
    };

    @Rule
    public TemporaryFolder tableRoot = new TemporaryFolder();

    /**
     * A batch lives in memory BE allocates through a native method BE registers, which no plain JVM
     * can link. Off heap has a switch for exactly this, and it swaps in plain Unsafe allocation.
     */
    @BeforeClass
    public static void allocateBatchesWithoutBe() {
        OffHeap.setTesting();
    }

    private static DorisPlugin loadPlugin() {
        List<DorisPlugin> found = new ArrayList<>();
        for (DorisPlugin plugin : ServiceLoader.load(DorisPlugin.class,
                HadoopHudiPluginTest.class.getClassLoader())) {
            found.add(plugin);
        }
        Assert.assertEquals("this module must declare exactly one DorisPlugin in META-INF/services",
                1, found.size());
        return found.get(0);
    }

    /** The path the plugin registry takes: services file, plugin class, factory list. */
    @Test
    public void isDiscoverableThroughServiceLoader() {
        Assert.assertTrue(loadPlugin() instanceof HadoopHudiPlugin);
    }

    /** "reader", not "hudi": a factory is named for its job inside the plugin. */
    @Test
    public void publishesItsScannerUnderThePublishedName() {
        List<String> names = new ArrayList<>();
        for (JniScannerFactory factory : loadPlugin().getScannerFactories()) {
            names.add(factory.getName());
        }
        Assert.assertEquals(Collections.singletonList("reader"), names);
    }

    /** A plugin declares only the kinds it provides; the rest stay empty rather than throwing. */
    @Test
    public void providesNeitherWritersNorUdfs() {
        Assert.assertFalse(loadPlugin().getWriterFactories().iterator().hasNext());
        Assert.assertFalse(loadPlugin().getUdfExecutorFactories().iterator().hasNext());
    }

    /**
     * Hudi tables are read through hadoop's FileSystem, whose implementations each live in their own
     * artifact. hadoop-common registers only file, har, http and viewfs; the rest used to come from
     * BE's system classpath - this was the one scanner that declared hadoop provided and read it
     * from lib/hadoop_hdfs - which a plugin cannot see, so a table on HDFS would end at "No
     * FileSystem for scheme: hdfs".
     *
     * <p>Unlike the closure caveat in this class's javadoc, this test does bite: the artifacts it
     * asserts are compile/runtime scope, so dropping one from the pom drops it from the test
     * classpath too. Which class serves which scheme is FE's choice - see the fe-filesystem property
     * classes, which point cosn and (in some configurations) obs at S3A rather than at a vendor
     * filesystem.
     *
     * <p>WHAT IT DOES NOT PROVE, and the distinction matters because it is easy to read this as more
     * than it is: {@code getFileSystemClass} RESOLVES a class, it does not LINK or initialize one. A
     * provider whose own dependencies are missing still passes here and fails on the first open. The
     * {@code wasb} entry is exactly that case today - {@code NativeAzureFileSystem} reaches
     * {@code AzureNativeFileSystemStore}, whose static initializer needs a jetty class the bundled
     * jetty 12 does not carry - and that is a pre-existing runtime state of the {@code wasb://}
     * scheme rather than anything this layout changed. The entry stays because the resolution is
     * still the thing a missing hadoop-azure would break; the deeper gap is what
     * {@code check_plugin_layout.py} covers by walking the constant pool of the bundled filesystem
     * jars themselves.
     */
    @Test
    public void resolvesEveryFilesystemSchemeAScanCanArriveOn() throws IOException {
        Configuration conf = new Configuration();
        Map<String, String> expected = new HashMap<>();
        expected.put("hdfs", "org.apache.hadoop.hdfs.DistributedFileSystem");
        expected.put("webhdfs", "org.apache.hadoop.hdfs.web.WebHdfsFileSystem");
        expected.put("s3a", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        expected.put("obs", "org.apache.hadoop.fs.obs.OBSFileSystem");
        expected.put("abfs", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem");
        expected.put("wasb", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
        for (Map.Entry<String, String> scheme : expected.entrySet()) {
            Assert.assertEquals(scheme.getKey() + ":// must resolve inside the plugin",
                    scheme.getValue(),
                    FileSystem.getFileSystemClass(scheme.getKey(), conf).getName());
        }
        // file:// is asserted by kind rather than by class, because isolation changed which
        // LocalFileSystem answers it. The repackaged hive declares a FileSystem provider of its
        // own, and its ProxyLocalFileSystem - a LocalFileSystem subclass that only overrides
        // rename - is listed after hadoop's, so it is what a plugin registers for file. It could
        // not before: hadoop fills that registry once, from the classloader of whoever asks first,
        // and that was BE with only its system classpath in view.
        Assert.assertTrue("file:// must be served by a local filesystem inside the plugin",
                org.apache.hadoop.fs.LocalFileSystem.class.isAssignableFrom(
                        FileSystem.getFileSystemClass("file", conf)));
    }

    /**
     * The four classes a Hive UDF is written against exist twice in a BE: hive-udf-shade cuts them
     * out of hive-exec 3.1.3 for java-udf, and the repackaged hive this scanner deserializes rows
     * with carries 3.1.2-22's. java-udf's jar sits on BE's system classpath, so parent-first used to
     * hand this scanner 3.1.3's copies of exactly those four and 3.1.2-22's of everything else -
     * a version split down the middle of one library, invisible until something behaved oddly.
     *
     * <p>A plugin resolves them in its own directory, so the copy that runs is the one this module
     * compiled against - hive-apache-shade, which is that repackaged hive with one copy of the
     * storage API rather than two. Asserting where they come from also catches a second jar in the
     * closure starting to carry them, which is the same failure again in a new place.
     */
    @Test
    public void readsTheHiveUdfContractFromTheRepackagedHive() throws ClassNotFoundException {
        String[] contract = {
                "org.apache.hadoop.hive.ql.exec.UDF",
                "org.apache.hadoop.hive.ql.exec.DefaultUDFMethodResolver",
                "org.apache.hadoop.hive.ql.exec.Description",
                "org.apache.hadoop.hive.ql.metadata.HiveException",
        };
        for (String name : contract) {
            String from = Class.forName(name).getProtectionDomain().getCodeSource()
                    .getLocation().getPath();
            Assert.assertTrue(name + " must come from hive-apache-shade, not " + from,
                    from.contains("hive-apache-shade"));
        }
    }

    /**
     * ORC is in this plugin because a Hudi table's base files can be ORC, and it is compiled against
     * the standalone hive-storage-api - whose column vectors have members the copy embedded in the
     * repackaged hive does not. It calls {@code TimestampColumnVector.changeCalendar} and treats a
     * {@code VectorizedRowBatch} as a {@code MutableFilterContext}; against the older copy those are
     * a NoSuchMethodError and a NoClassDefFoundError on the first ORC read.
     *
     * <p>Carrying both jars is the natural state of a Hudi closure, and which copy answered used to
     * be settled by whichever one a fat jar assembly unpacked last. They are merged in
     * hive-apache-shade instead, and this asserts that the merge kept the copies ORC needs - by
     * behaviour rather than by version string, because a version string does not say which copy a
     * classloader reached first.
     */
    @Test
    public void keepsTheStorageApiThatOrcIsCompiledAgainst() throws NoSuchMethodException {
        Assert.assertTrue("a VectorizedRowBatch must be a MutableFilterContext",
                MutableFilterContext.class.isAssignableFrom(VectorizedRowBatch.class));
        Assert.assertNotNull("TimestampColumnVector must be able to change calendar",
                TimestampColumnVector.class.getMethod("changeCalendar", boolean.class,
                        boolean.class));
    }

    /**
     * End to end over the real wire format: FE names a base file, the hive input format to read it
     * with and the serde to deserialize rows with, BE passes all three as parameters, and rows come
     * out. Everything the read needs has to be inside the plugin - hudi-hadoop-mr for the input
     * format, hive for the serde and its object inspectors, parquet to decode the file and hadoop's
     * mapred API underneath all of it - and this is the only test here that would notice one of them
     * missing rather than merely mis-declared.
     */
    @Test
    public void readsBackWhatItWrote() throws Exception {
        File baseFile = createCopyOnWriteTableWithTwoRows();

        Map<String, String> params = new HashMap<>();
        params.put("base_path", tableRoot.getRoot().getAbsolutePath());
        params.put("data_file_path", baseFile.getAbsolutePath());
        params.put("data_file_length", String.valueOf(baseFile.length()));
        params.put("delta_file_paths", "");
        params.put("instant_time", INSTANT);
        params.put("hudi_column_names", String.join(",", COLUMNS));
        params.put("hudi_column_types", "string#string#string#string#string#int#string");
        params.put("required_fields", "id");
        params.put("input_format", "org.apache.hudi.hadoop.HoodieParquetInputFormat");
        params.put("serde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        params.put("time_zone", "UTC");

        JniScanner scanner = loadPlugin().getScannerFactories().iterator().next().create(16, params);
        scanner.open();
        try {
            Assert.assertNotEquals(0, scanner.getNextBatchMeta());
            Assert.assertEquals(2, scanner.getTable().getNumRows());
            scanner.releaseTable();
            Assert.assertEquals("0 means end of stream", 0, scanner.getNextBatchMeta());
        } finally {
            scanner.close();
        }
    }

    /**
     * A Hudi table is a directory of data files plus the {@code .hoodie} metadata that makes them a
     * table: the input format reads the schema off the timeline before it reads a row, so a base
     * file on its own is not enough. Written here without the Hudi write client, which lives in
     * hudi-client and is not something a reader has any business depending on.
     */
    private File createCopyOnWriteTableWithTwoRows() throws IOException {
        String basePath = tableRoot.getRoot().getAbsolutePath();
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.newTableBuilder()
                .setTableType(HoodieTableType.COPY_ON_WRITE)
                .setTableName("t")
                .initTable(new HadoopStorageConfiguration(new Configuration()), basePath);

        StringBuilder fields = new StringBuilder();
        for (String metaField : Arrays.copyOfRange(COLUMNS, 0, 5)) {
            fields.append("{\"name\":\"").append(metaField).append("\",\"type\":\"string\"},");
        }
        Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"t\",\"fields\":["
                + fields
                + "{\"name\":\"id\",\"type\":\"int\"},"
                + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null}]}");
        File baseFile = new File(tableRoot.getRoot(),
                FSUtils.makeBaseFileName(INSTANT, "0-0-0", FILE_ID, ".parquet"));
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(new Path(baseFile.getAbsolutePath()))
                .withSchema(schema)
                .withConf(new Configuration())
                .build()) {
            for (int id = 1; id <= 2; id++) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("_hoodie_commit_time", INSTANT);
                record.put("_hoodie_commit_seqno", INSTANT + "_0_" + id);
                record.put("_hoodie_record_key", String.valueOf(id));
                record.put("_hoodie_partition_path", "");
                record.put("_hoodie_file_name", baseFile.getName());
                record.put("id", id);
                record.put("name", "row" + id);
                writer.write(record);
            }
        }

        commit(metaClient, baseFile, schema);
        return baseFile;
    }

    /** One completed commit naming the base file: requested, inflight, then complete. */
    private void commit(HoodieTableMetaClient metaClient, File baseFile, Schema schema) {
        HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
        InstantGenerator instants = metaClient.getInstantGenerator();
        timeline.createNewInstant(
                instants.createNewInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, INSTANT));
        timeline.transitionRequestedToInflight(
                instants.createNewInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, INSTANT),
                Option.empty());

        HoodieWriteStat stat = new HoodieWriteStat();
        stat.setFileId(FILE_ID);
        stat.setPath(baseFile.getName());
        stat.setPartitionPath("");
        HoodieCommitMetadata metadata = new HoodieCommitMetadata();
        metadata.setOperationType(WriteOperationType.INSERT);
        metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, schema.toString());
        metadata.addWriteStat("", stat);
        timeline.saveAsComplete(
                instants.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, INSTANT),
                Option.of(metadata));
    }
}
