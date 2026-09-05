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

import org.apache.doris.jni.spi.utils.OffHeap;
import org.apache.doris.jni.spi.vec.ColumnType;
import org.apache.doris.jni.spi.vec.VectorTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * The writer half of what {@link JniScannerContractTest} does for scanners: the exact method set BE
 * resolves on {@link JniWriter}, and the classloader those methods run under.
 *
 * <p>It exists because PROTOCOL.md claimed this and nothing delivered it. The four writer method
 * ids in {@code PluginRegistry::_init_writer_api} had no gate at all - renaming
 * {@code JniWriter.write} broke every JNI writer at runtime with every test still green.
 */
class JniWriterContractTest {

    @BeforeAll
    static void useUnsafeAllocation() {
        // The memory tracker natives are registered by BE; in a unit test there is no BE.
        OffHeap.setTesting();
    }

    /**
     * BE resolves these method ids on JniWriter itself, so renaming or re-signing one of them
     * breaks every JNI writer at once. The C++ side that must be changed together with this list is
     * {@code PluginRegistry::_init_writer_api} in be/src/util/jni_plugin_registry.cpp.
     */
    @Test
    void beResolvedMethodsKeepTheirSignature() throws NoSuchMethodException {
        assertSignature("open", void.class);
        assertSignature("write", void.class, Map.class);
        assertSignature("getStatistics", Map.class);
        assertSignature("close", void.class);
    }

    /**
     * Every entry point is final, for the same reason as on the scanner: an override would run on
     * BE's thread with BE's context classloader, and the failure would surface deep inside some
     * library as a class-not-found with no hint about the cause.
     */
    @Test
    void beResolvedMethodsAreFinalSoTheClassLoaderGuaranteeHolds() {
        List<String> overridable = new ArrayList<>();
        for (Method method : JniWriter.class.getDeclaredMethods()) {
            if (Modifier.isPublic(method.getModifiers()) && !Modifier.isFinal(method.getModifiers())
                    && !Modifier.isStatic(method.getModifiers())) {
                overridable.add(method.getName());
            }
        }
        Assertions.assertEquals(Collections.emptyList(), overridable,
                "public non-final methods on JniWriter can be overridden, which would skip the"
                        + " context classloader that BE's attached threads do not set up themselves");
    }

    /**
     * BE calls in on threads it created and attached to the JVM, whose context classloader is BE's
     * own and cannot see a single plugin class. Every entry point must install the plugin's loader
     * and put back what was there before.
     */
    @Test
    void everyEntryPointRunsUnderThePluginClassLoaderAndRestoresIt() throws IOException {
        ClassLoader beThreadLoader = new URLClassLoader(new URL[0], null);
        Thread.currentThread().setContextClassLoader(beThreadLoader);
        VectorTable block = emptyBlock();
        try {
            RecordingWriter writer = new RecordingWriter();
            ClassLoader pluginLoader = RecordingWriter.class.getClassLoader();

            writer.open();
            writer.write(blockParams(block));
            writer.getStatistics();
            writer.close();

            Assertions.assertEquals(
                    new TreeSet<>(Arrays.asList("openInternal", "writeInternal",
                            "collectStatistics", "closeInternal")),
                    new TreeSet<>(writer.observed.keySet()),
                    "an entry point stopped reaching its plugin hook");
            for (Map.Entry<String, ClassLoader> observed : writer.observed.entrySet()) {
                Assertions.assertSame(pluginLoader, observed.getValue(),
                        observed.getKey() + " ran under the wrong context classloader");
            }
            Assertions.assertSame(beThreadLoader, Thread.currentThread().getContextClassLoader(),
                    "the caller's context classloader must be restored on the way out");
        } finally {
            block.close();
            Thread.currentThread().setContextClassLoader(null);
        }
    }

    /** A hook that throws must still restore the caller's classloader. */
    @Test
    void failingHookStillRestoresTheContextClassLoader() {
        ClassLoader beThreadLoader = new URLClassLoader(new URL[0], null);
        Thread.currentThread().setContextClassLoader(beThreadLoader);
        try {
            RecordingWriter writer = new RecordingWriter();
            writer.failOnOpen = true;
            try {
                writer.open();
                Assertions.fail("openInternal was supposed to throw");
            } catch (IOException expected) {
                Assertions.assertEquals("boom", expected.getMessage());
            }
            Assertions.assertSame(beThreadLoader, Thread.currentThread().getContextClassLoader());
        } finally {
            Thread.currentThread().setContextClassLoader(null);
        }
    }

    /** The block BE hands write(): a zero-column table, which is enough to exercise the contract. */
    private static VectorTable emptyBlock() {
        return VectorTable.createWritableTable(new ColumnType[0], new String[0], 4);
    }

    private static Map<String, String> blockParams(VectorTable block) {
        Map<String, String> params = new HashMap<>();
        params.put("required_fields", "");
        params.put("columns_types", "");
        params.put("meta_address", String.valueOf(block.getMetaAddress()));
        return params;
    }

    private static void assertSignature(String name, Class<?> returnType, Class<?>... params)
            throws NoSuchMethodException {
        Method method = JniWriter.class.getMethod(name, params);
        Assertions.assertEquals(returnType, method.getReturnType(), name + " changed its return type");
        Assertions.assertEquals(JniWriter.class, method.getDeclaringClass(),
                name + " must be declared on JniWriter: BE resolves its method id there, not on the"
                        + " concrete plugin class");
    }

    private static final class RecordingWriter extends JniWriter {
        private final Map<String, ClassLoader> observed = new HashMap<>();
        private boolean failOnOpen;

        private RecordingWriter() {
            super(4, Collections.emptyMap());
        }

        @Override
        protected void openInternal() throws IOException {
            observed.put("openInternal", Thread.currentThread().getContextClassLoader());
            if (failOnOpen) {
                throw new IOException("boom");
            }
        }

        @Override
        protected void writeInternal(VectorTable inputTable) {
            observed.put("writeInternal", Thread.currentThread().getContextClassLoader());
        }

        @Override
        protected void closeInternal() {
            observed.put("closeInternal", Thread.currentThread().getContextClassLoader());
        }

        @Override
        protected Map<String, String> collectStatistics() {
            observed.put("collectStatistics", Thread.currentThread().getContextClassLoader());
            return Collections.singletonMap("counter:rows", "1");
        }
    }
}
