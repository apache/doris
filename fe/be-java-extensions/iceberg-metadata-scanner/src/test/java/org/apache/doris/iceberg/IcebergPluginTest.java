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

package org.apache.doris.iceberg;

import org.apache.doris.jni.spi.DorisPlugin;
import org.apache.doris.jni.spi.JniScanner;
import org.apache.doris.jni.spi.JniScannerFactory;
import org.apache.doris.jni.spi.utils.OffHeap;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * BE addresses this plugin by name and never by class name, so none of that is checked at compile
 * time: a services file naming a class that moved, or a factory renamed, compiles and then fails as
 * "plugin iceberg has no factory named ..." on the query that needed it. The names asserted here
 * are the deployment contract with the table in BE's jni_plugin_registry.h.
 *
 * <p>The scan test drives the real wire format: it plans a metadata-table scan the way FE does,
 * ships the task across as the same base64 string, and reads the rows back out of it.
 *
 * <p>What it does <em>not</em> check is whether the deployed plugin directory contains everything
 * that scan needed. Surefire puts {@code provided} dependencies on the test classpath, so this
 * whole file still passes with, say, hadoop marked provided and therefore absent from the plugin
 * directory. Only loading the deployed directory through the plugin registry - see the acceptance
 * recipe in the migration notes - can catch that.
 */
public class IcebergPluginTest {

    /**
     * A batch lives in memory BE allocates through a native method BE registers, which no plain JVM
     * can link. Off heap has a switch for exactly this, and it swaps in plain Unsafe allocation.
     */
    @BeforeAll
    public static void allocateBatchesWithoutBe() {
        OffHeap.setTesting();
    }

    private static DorisPlugin loadPlugin() {
        List<DorisPlugin> found = new ArrayList<>();
        for (DorisPlugin plugin : ServiceLoader.load(DorisPlugin.class,
                IcebergPluginTest.class.getClassLoader())) {
            found.add(plugin);
        }
        Assertions.assertEquals(1, found.size(),
                "this module must declare exactly one DorisPlugin in META-INF/services");
        return found.get(0);
    }

    /** The path the plugin registry takes: services file, plugin class, factory list. */
    @Test
    public void isDiscoverableThroughServiceLoader() {
        Assertions.assertTrue(loadPlugin() instanceof IcebergPlugin);
    }

    /**
     * Not "reader": iceberg data files are read natively, and only the metadata tables come through
     * Java. Renaming this to match the other connectors would leave BE asking for a factory that
     * does not exist.
     */
    @Test
    public void publishesItsScannerUnderThePublishedName() {
        List<String> names = new ArrayList<>();
        for (JniScannerFactory factory : loadPlugin().getScannerFactories()) {
            names.add(factory.getName());
        }
        Assertions.assertEquals(java.util.Collections.singletonList("sys-table"), names);
    }

    /** A plugin declares only the kinds it provides; the rest stay empty rather than throwing. */
    @Test
    public void providesNeitherWritersNorUdfs() {
        Assertions.assertFalse(loadPlugin().getWriterFactories().iterator().hasNext());
        Assertions.assertFalse(loadPlugin().getUdfExecutorFactories().iterator().hasNext());
    }

    /**
     * End to end over the real wire format: FE serializes an iceberg FileScanTask to base64 and BE
     * hands it back as the {@code serialized_split} parameter. Everything the plugin needs to turn
     * that string into rows - iceberg-core, its avro manifest reader, hadoop for the FileIO the task
     * carries - has to be inside the plugin directory, which is what this asserts by doing it.
     */
    @Test
    public void scansAMetadataTableFromASerializedTask(@TempDir Path warehouse) throws IOException {
        String serializedTask = planOneSnapshotsTask(warehouse);

        Map<String, String> params = new HashMap<>();
        params.put("serialized_split", serializedTask);
        params.put("required_fields", "snapshot_id");
        params.put("required_types", "bigint");
        params.put("time_zone", "UTC");

        JniScanner scanner = loadPlugin().getScannerFactories().iterator().next().create(16, params);
        scanner.open();
        try {
            Assertions.assertNotEquals(0, scanner.getNextBatchMeta(), "the one snapshot row");
            Assertions.assertEquals(1, scanner.getTable().getNumRows());
            scanner.releaseTable();
            Assertions.assertEquals(0, scanner.getNextBatchMeta(), "0 means end of stream");
        } finally {
            scanner.close();
        }
    }

    /**
     * Builds a table with one snapshot and plans its {@code $snapshots} metadata table the way
     * IcebergScanPlanProvider.doPlanSystemTableScan does, down to projecting the scan to exactly the
     * requested column - the projection the scanner's positional row reads depend on.
     */
    private static String planOneSnapshotsTask(Path warehouse) throws IOException {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Table table = new HadoopTables(new Configuration())
                .create(schema, PartitionSpec.unpartitioned(), warehouse.resolve("t").toString());
        table.newAppend()
                .appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath(warehouse.resolve("t/data/f.parquet").toString())
                        .withFileSizeInBytes(10)
                        .withRecordCount(1)
                        .build())
                .commit();

        Table snapshots = MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.SNAPSHOTS);
        TableScan scan = snapshots.newScan()
                .project(new Schema(snapshots.schema().findField("snapshot_id")));
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            List<FileScanTask> planned = new ArrayList<>();
            tasks.forEach(planned::add);
            Assertions.assertEquals(1, planned.size(), "one snapshot, one task");
            return SerializationUtil.serializeToBase64(planned.get(0));
        }
    }
}
