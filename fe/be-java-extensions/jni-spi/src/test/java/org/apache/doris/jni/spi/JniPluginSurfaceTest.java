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

package org.apache.doris.jni.spi;

import org.apache.doris.jni.spi.utils.JNINativeMethod;
import org.apache.doris.jni.spi.utils.JniUtil;
import org.apache.doris.jni.spi.utils.OffHeap;
import org.apache.doris.jni.spi.utils.TypeNativeBytes;
import org.apache.doris.jni.spi.vec.ColumnType;
import org.apache.doris.jni.spi.vec.ColumnValue;
import org.apache.doris.jni.spi.vec.ColumnValueConverter;
import org.apache.doris.jni.spi.vec.NativeColumnValue;
import org.apache.doris.jni.spi.vec.VectorColumn;
import org.apache.doris.jni.spi.vec.VectorTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

/**
 * Freezes the JNI plugin API surface, so that changing it cannot happen without also deciding the
 * version consequence.
 *
 * <p><b>Why this exists.</b> {@code <jni.plugin.api.version>} in {@code fe/be-java-extensions/pom.xml}
 * decides which BE a given plugin directory may load into: it is stamped into every plugin jar's
 * {@code Doris-Jni-Plugin-Api-Version} manifest attribute and compared, major against major, by
 * {@code PluginRuntime}. The rule attached to it is blunt - <em>any</em> change to the surface below,
 * adding a type or a method just as much as removing or re-signing one, is a MAJOR change - and
 * nothing enforces it: every plugin in this repo is rebuilt from the same tree, so a surface change
 * with no bump compiles, tests green, and only breaks a plugin built somewhere else.
 *
 * <p>{@link JniScannerContractTest} covers a narrower question - the ten signatures BE resolves by
 * name on {@code JniScanner} - and it exists because those break loudly at scan time. This one is
 * about the whole shared layer, including the parts BE never touches but plugins compile against.
 *
 * <p><b>Regenerating.</b> Run this test, copy the "actual" block out of the failure message into
 * {@code src/test/resources/jni-plugin-surface.txt}, and bump the major of
 * {@code jni.plugin.api.version} in {@code fe/be-java-extensions/pom.xml} in the SAME commit.
 *
 * <p>Signatures are recorded with their return type: a changed return type is a MAJOR change by the
 * same definition, and a name-and-parameters-only record cannot see it.
 */
public class JniPluginSurfaceTest {

    private static final String BASELINE_RESOURCE = "/jni-plugin-surface.txt";

    /**
     * Pins the literal so that the version and the baseline below move together. Regenerating the
     * baseline without touching this line, or the other way round, is the mistake this catches.
     */
    @Test
    public void jniPluginApiMajorTracksTheRecordedSurfaceChange() {
        // getTableSchema/parseTableSchema removed (never implemented by any plugin and never
        // called by BE), and JniWriter no longer parses required_fields/columns_types into two
        // fields no writer read.
        Assertions.assertEquals("3.0", SpiVersion.version());
    }

    /** Everything a plugin compiles against: the entry points, the factories, the data plane. */
    private static final List<Class<?>> FROZEN_TYPES = Arrays.asList(
            DorisPlugin.class,
            JniScanner.class,
            JniScannerFactory.class,
            JniWriter.class,
            JniWriterFactory.class,
            UdfExecutorFactory.class,
            SpiVersion.class,
            ThreadContextClassLoader.class,
            JNINativeMethod.class,
            JniUtil.class,
            OffHeap.class,
            TypeNativeBytes.class,
            ColumnType.class,
            ColumnValue.class,
            ColumnValueConverter.class,
            NativeColumnValue.class,
            VectorColumn.class,
            VectorTable.class);

    @Test
    public void pluginApiSurfaceMatchesRecordedBaseline() throws IOException {
        TreeSet<String> actual = renderSurface();
        TreeSet<String> expected = readBaseline();

        TreeSet<String> missing = new TreeSet<>(expected);
        missing.removeAll(actual);
        TreeSet<String> added = new TreeSet<>(actual);
        added.removeAll(expected);

        Assertions.assertTrue(missing.isEmpty() && added.isEmpty(),
                "The JNI plugin API surface changed.\n"
                        + "  gone from the baseline (removed, renamed, or re-signed): " + missing + "\n"
                        + "  new since the baseline: " + added + "\n"
                        + "THIS IS A MAJOR CHANGE - the same commit that refreshes src/test/resources"
                        + BASELINE_RESOURCE + " must increment the major of <jni.plugin.api.version>"
                        + " in fe/be-java-extensions/pom.xml (and zero its minor), and the literal in"
                        + " jniPluginApiMajorTracksTheRecordedSurfaceChange above.\n"
                        + "Full actual surface:\n" + String.join("\n", actual));
    }

    /**
     * One line per method reachable on a frozen type, keyed by that type rather than by the class
     * that happens to declare it: what matters is what a plugin can call on the type it was handed.
     * Protected members are included as well - a plugin subclasses {@code JniScanner} and
     * {@code JniWriter}, so their protected hooks are as much of its contract as the public ones.
     */
    private static TreeSet<String> renderSurface() {
        TreeSet<String> rendered = new TreeSet<>();
        for (Class<?> frozen : FROZEN_TYPES) {
            for (Class<?> level = frozen; level != null && level != Object.class; level = level.getSuperclass()) {
                for (Method m : level.getDeclaredMethods()) {
                    if (m.isSynthetic() || java.lang.reflect.Modifier.isPrivate(m.getModifiers())) {
                        continue;
                    }
                    StringBuilder sb = new StringBuilder(frozen.getName()).append('#')
                            .append(m.getName()).append('(');
                    Class<?>[] params = m.getParameterTypes();
                    for (int i = 0; i < params.length; i++) {
                        if (i > 0) {
                            sb.append(',');
                        }
                        sb.append(params[i].getTypeName());
                    }
                    rendered.add(sb.append("):").append(m.getReturnType().getTypeName()).toString());
                }
            }
        }
        return rendered;
    }

    private static TreeSet<String> readBaseline() throws IOException {
        TreeSet<String> baseline = new TreeSet<>();
        try (InputStream in = JniPluginSurfaceTest.class.getResourceAsStream(BASELINE_RESOURCE)) {
            Assertions.assertNotNull(in, "missing test resource " + BASELINE_RESOURCE);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                String entry = line.trim();
                if (!entry.isEmpty() && !entry.startsWith("#")) {
                    baseline.add(entry);
                }
            }
        }
        return baseline;
    }
}
