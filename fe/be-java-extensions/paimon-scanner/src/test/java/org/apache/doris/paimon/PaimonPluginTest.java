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

package org.apache.doris.paimon;

import org.apache.doris.jni.spi.DorisPlugin;
import org.apache.doris.jni.spi.JniScanner;
import org.apache.doris.jni.spi.JniScannerFactory;
import org.apache.doris.jni.spi.utils.OffHeap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * BE addresses this plugin by name and never by class name, so none of that is checked at compile
 * time: a services file naming a class that moved, or a factory renamed, compiles and then fails as
 * "plugin paimon has no factory named reader" on the query that needed it. The names asserted here
 * are the deployment contract with the table in BE's jni_plugin_registry.h.
 *
 * <p>What this file does <em>not</em> check is whether the deployed plugin directory contains
 * everything a scan needs. Surefire puts {@code provided} dependencies on the test classpath, so
 * these tests would still pass with a dependency marked provided and therefore absent from the
 * plugin directory. Only loading the deployed directory through the plugin registry - see the
 * acceptance recipe in the migration notes - can catch that.
 */
public class PaimonPluginTest {

    @Rule
    public TemporaryFolder warehouse = new TemporaryFolder();

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
                PaimonPluginTest.class.getClassLoader())) {
            found.add(plugin);
        }
        Assert.assertEquals("this module must declare exactly one DorisPlugin in META-INF/services",
                1, found.size());
        return found.get(0);
    }

    /** The path the plugin registry takes: services file, plugin class, factory list. */
    @Test
    public void isDiscoverableThroughServiceLoader() {
        Assert.assertTrue(loadPlugin() instanceof PaimonPlugin);
    }

    /** "reader", not "paimon": a factory is named for its job inside the plugin. */
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

    /** The factory builds the scanner rather than merely naming it. */
    @Test
    public void buildsTheScanner() {
        Map<String, String> params = new HashMap<>();
        params.put("required_fields", "id");
        params.put("columns_types", "int");
        params.put("serialized_table_cache_key", "k");
        Assert.assertTrue(loadPlugin().getScannerFactories().iterator().next().create(1024, params)
                instanceof PaimonJniScanner);
    }

    /**
     * Paimon serves s3:// from its own bundled FileIO and everything else through hadoop, whose
     * FileSystem implementations each live in their own artifact. hadoop-common registers only
     * file, har, http and viewfs; the rest used to come from BE's system classpath, which a plugin
     * cannot see - a warehouse on HDFS would end at "No FileSystem for scheme: hdfs".
     *
     * <p>Unlike the closure caveat in this class's javadoc, this test does bite: the artifacts it
     * asserts are compile/runtime scope, so dropping one from the pom drops it from the test
     * classpath too. Which class serves which scheme is FE's choice - see the fe-filesystem
     * property classes, which point cosn and (in some configurations) obs at S3A rather than at a
     * vendor filesystem.
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
        expected.put("file", "org.apache.hadoop.fs.LocalFileSystem");
        for (Map.Entry<String, String> scheme : expected.entrySet()) {
            Assert.assertEquals(scheme.getKey() + ":// must resolve inside the plugin",
                    scheme.getValue(),
                    FileSystem.getFileSystemClass(scheme.getKey(), conf).getName());
        }
    }

    /**
     * Paimon picks a FileIO by scheme through its own ServiceLoader, and a provider must be able to
     * see the interface it implements. Both of these used to sit on the shared preload classpath
     * while paimon-common - which defines FileIOLoader - did not, which is a NoClassDefFoundError
     * at discovery time; inside a plugin directory they are simply together.
     */
    @Test
    public void registersItsObjectStoreFileIoLoaders() {
        List<String> schemes = new ArrayList<>();
        for (org.apache.paimon.fs.FileIOLoader loader : ServiceLoader.load(
                org.apache.paimon.fs.FileIOLoader.class, PaimonPluginTest.class.getClassLoader())) {
            schemes.add(loader.getScheme());
        }
        Assert.assertTrue("paimon-s3 provides s3://, seen: " + schemes, schemes.contains("s3"));
        Assert.assertTrue("paimon-jindo provides oss://, seen: " + schemes, schemes.contains("oss"));
    }

    /**
     * End to end over the real wire format: FE serializes the table and one split, BE hands both
     * back as parameters, and rows come out. Everything the read needs has to be inside the plugin
     * - paimon-core to plan it, paimon-format to decode the data file, hadoop for the filesystem -
     * and this is the only test here that would notice one of them missing rather than merely
     * mis-declared.
     */
    @Test
    public void readsBackWhatItWrote() throws Exception {
        Table table = createTableWithTwoRows();
        List<Split> splits = table.newReadBuilder().newScan().plan().splits();
        Assert.assertEquals("one bucket, one split", 1, splits.size());

        Map<String, String> params = new HashMap<>();
        params.put("serialized_table", encode(table));
        params.put("paimon_split", encode(splits.get(0)));
        params.put("required_fields", "id");
        params.put("columns_types", "int");
        params.put("time_zone", "UTC");
        params.put("serialized_table_cache_key", "readsBackWhatItWrote");

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

    private Table createTableWithTwoRows() throws Exception {
        Options options = new Options();
        options.set("warehouse", warehouse.getRoot().getAbsolutePath());
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase("db", true);
        Identifier identifier = Identifier.create("db", "t");
        catalog.createTable(identifier, Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .option("bucket", "1")
                .option("bucket-key", "id")
                .build(), true);
        Table table = catalog.getTable(identifier);

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(1, BinaryString.fromString("one")));
            write.write(GenericRow.of(2, BinaryString.fromString("two")));
            commit.commit(write.prepareCommit());
        }
        return table;
    }

    /** The encoding the scanner decodes with: paimon's own java serialization, then base64. */
    private static String encode(Object value) throws IOException {
        return Base64.getEncoder().encodeToString(InstantiationUtil.serializeObject(value));
    }
}
