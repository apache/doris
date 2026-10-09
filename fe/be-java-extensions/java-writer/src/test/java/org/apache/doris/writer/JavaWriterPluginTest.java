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

package org.apache.doris.writer;

import org.apache.doris.jni.spi.DorisPlugin;
import org.apache.doris.jni.spi.JniWriter;
import org.apache.doris.jni.spi.JniWriterFactory;
import org.apache.doris.jni.spi.utils.OffHeap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * BE reaches this plugin by name, never by class name, so nothing here is checked at compile
 * time: a services file naming a class that moved, or a factory whose name was tidied up, both
 * compile and then fail as "plugin has no factory named ..." on the query that needs it.
 */
public class JavaWriterPluginTest {

    @BeforeAll
    public static void useTestAllocator() {
        // Off-heap allocation normally goes through native methods BE registers into the JVM it
        // created. There is no BE here, so the fallback allocator has to be selected explicitly.
        OffHeap.setTesting();
    }

    private static DorisPlugin loadPlugin() {
        List<DorisPlugin> found = new ArrayList<>();
        for (DorisPlugin plugin : ServiceLoader.load(DorisPlugin.class,
                JavaWriterPluginTest.class.getClassLoader())) {
            found.add(plugin);
        }
        Assertions.assertEquals(1, found.size(),
                "this module must declare exactly one DorisPlugin in META-INF/services");
        return found.get(0);
    }

    /** The path the plugin registry takes: services file, plugin class, factory list. */
    @Test
    public void isDiscoverableThroughServiceLoader() {
        Assertions.assertTrue(loadPlugin() instanceof JavaWriterPlugin);
    }

    /**
     * The factory name is half of the pair BE addresses, so it is as much a published name as a
     * file format is. Renaming it is a deployment-breaking change, not a refactor.
     */
    @Test
    public void publishesTheLocalFileWriterUnderItsPublishedName() {
        List<String> names = new ArrayList<>();
        for (JniWriterFactory factory : loadPlugin().getWriterFactories()) {
            names.add(factory.getName());
        }
        Assertions.assertEquals(java.util.Collections.singletonList("local-file"), names);
    }

    /** A plugin declares only the kinds it provides; the rest stay empty rather than throwing. */
    @Test
    public void providesNoScannersOrUdfs() {
        Assertions.assertTrue(!loadPlugin().getScannerFactories().iterator().hasNext());
        Assertions.assertTrue(!loadPlugin().getUdfExecutorFactories().iterator().hasNext());
    }

    /**
     * End to end through the SPI entry points, which is the only way the writer is ever called:
     * the factory builds it, {@code write} is handed the address of an off-heap block, and the
     * rows come out separated the way the parameters asked for.
     */
    @Test
    public void writesTheBlockItIsHandedThroughTheSpiEntryPoints(@TempDir Path dir)
            throws IOException {
        Path out = dir.resolve("out.csv");
        Map<String, String> params = new HashMap<>();
        params.put("file_path", out.toString());
        params.put("column_separator", "|");

        JniWriter writer = new LocalFileWriterFactory().create(1024, params);
        writer.open();
        writer.write(blockOf(new Integer[] {1, 2}, new Integer[] {7, null}));
        writer.close();

        // The null is the writer's own rendering, not the data: a column with no value has to be
        // distinguishable from a column holding the text "null".
        Assertions.assertEquals("1|7\n2|\\N\n",
                new String(Files.readAllBytes(out), StandardCharsets.UTF_8));
    }

    /**
     * Statistics reach the query profile through a hook the base class calls, and a subclass that
     * simply did not implement it reports nothing while everything else keeps working - the one
     * way this port could have gone wrong without failing to compile.
     */
    @Test
    public void reportsItsCountersThroughTheStatisticsHook(@TempDir Path dir) throws IOException {
        Path out = dir.resolve("out.csv");
        Map<String, String> params = new HashMap<>();
        params.put("file_path", out.toString());

        JniWriter writer = new LocalFileWriterFactory().create(1024, params);
        writer.open();
        writer.write(blockOf(new Integer[] {1, 2}, new Integer[] {7, 8}));
        Map<String, String> stats = writer.getStatistics();
        writer.close();

        Assertions.assertEquals("2", stats.get("counter:WrittenRows"));
        Assertions.assertEquals(String.valueOf("1,7\n2,8\n".length()), stats.get("bytes:WrittenBytes"));
        Assertions.assertNotNull(stats.get("timer:WriteTime"));
        Assertions.assertNotNull(stats.get("timer:ReadTableTime"));
    }

    /**
     * Builds the meta array of one block exactly as BE does, per PROTOCOL.md section 5: a row
     * count, then per column a const flag, a null map address and a data address.
     *
     * <p>It is built by hand rather than by asking a writable VectorTable for its meta address,
     * because the two are not the same layout. A writable table publishes what the C++ *scanner*
     * reader consumes, which has no const-flag word; a block handed to a writer comes from
     * JniDataBridge::_fill_column_meta, which has one. Only the second is what this class is ever
     * given, so only the second is worth testing against.
     */
    private static Map<String, String> blockOf(Integer[] first, Integer[] second) {
        int rows = first.length;
        long meta = OffHeap.allocateMemory((1 + 2 * 3) * 8L);
        long cursor = meta;
        OffHeap.putLong(null, cursor, rows);
        cursor += 8;
        for (Integer[] values : new Integer[][] {first, second}) {
            OffHeap.putLong(null, cursor, 0);
            cursor += 8;
            OffHeap.putLong(null, cursor, nullMapOf(values));
            cursor += 8;
            OffHeap.putLong(null, cursor, dataOf(values));
            cursor += 8;
        }

        Map<String, String> params = new HashMap<>();
        params.put("required_fields", "a,b");
        params.put("columns_types", "int#int");
        params.put("meta_address", String.valueOf(meta));
        params.put("num_rows", String.valueOf(rows));
        return params;
    }

    /** One byte per row, non-zero where the row is null; address 0 when the column has none. */
    private static long nullMapOf(Integer[] values) {
        boolean anyNull = false;
        for (Integer value : values) {
            anyNull |= value == null;
        }
        if (!anyNull) {
            return 0;
        }
        long address = OffHeap.allocateMemory(values.length);
        for (int i = 0; i < values.length; i++) {
            OffHeap.putByte(null, address + i, (byte) (values[i] == null ? 1 : 0));
        }
        return address;
    }

    private static long dataOf(Integer[] values) {
        long address = OffHeap.allocateMemory(values.length * 4L);
        for (int i = 0; i < values.length; i++) {
            OffHeap.putInt(null, address + i * 4L, values[i] == null ? 0 : values[i]);
        }
        return address;
    }
}
