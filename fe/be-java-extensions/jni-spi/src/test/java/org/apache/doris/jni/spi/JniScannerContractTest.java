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
 * Guards the two properties of {@link JniScanner} that BE depends on and a plugin author cannot
 * see: the exact method set BE resolves, and the classloader those methods run under.
 */
class JniScannerContractTest {

    @BeforeAll
    static void useUnsafeAllocation() {
        // The memory tracker natives are registered by BE; in a unit test there is no BE.
        OffHeap.setTesting();
    }

    /**
     * BE resolves these method ids on JniScanner itself, so renaming or re-signing one of them
     * breaks every plugin at once with a NoSuchMethodError at scan time. The C++ side that must be
     * changed together with this list is be/src/format/jni/jni_reader.cpp and
     * be/src/format_v2/jni/jni_table_reader.cpp.
     */
    @Test
    void beResolvedMethodsKeepTheirSignature() throws NoSuchMethodException {
        assertSignature("open", void.class);
        assertSignature("close", void.class);
        assertSignature("getNextBatchMeta", long.class);
        assertSignature("getTableSchema", String.class);
        assertSignature("getStatistics", Map.class);
        assertSignature("getAppendDataTime", long.class);
        assertSignature("getCreateVectorTableTime", long.class);
        assertSignature("setBatchSize", void.class, int.class);
        assertSignature("releaseColumn", void.class, int.class);
        assertSignature("releaseTable", void.class);
    }

    /**
     * Every entry point is final. This is what makes the classloader guarantee below unconditional:
     * if a plugin could override open() or getStatistics(), its body would run on BE's thread with
     * BE's context classloader, and the failure would surface deep inside some library as a
     * class-not-found with no hint about the cause.
     */
    @Test
    void beResolvedMethodsAreFinalSoTheClassLoaderGuaranteeHolds() {
        List<String> overridable = new ArrayList<>();
        for (Method method : JniScanner.class.getDeclaredMethods()) {
            if (Modifier.isPublic(method.getModifiers()) && !Modifier.isFinal(method.getModifiers())
                    && !Modifier.isStatic(method.getModifiers())
                    // getTable/resetTable are helpers for subclasses and tests, not BE entry points.
                    && !method.getName().equals("getTable") && !method.getName().equals("resetTable")) {
                overridable.add(method.getName());
            }
        }
        Assertions.assertEquals(Collections.emptyList(), overridable,
                "public non-final methods on JniScanner can be overridden, which would skip the"
                        + " context classloader that BE's attached threads do not set up themselves");
    }

    /**
     * BE calls in on threads it created and attached to the JVM, whose context classloader is BE's
     * own - under plugin isolation that classloader cannot see a single plugin class. Libraries
     * inside plugins resolve ServiceLoader providers through the context classloader, so each entry
     * point must install the plugin's loader and put back what was there before.
     */
    @Test
    void everyEntryPointRunsUnderThePluginClassLoaderAndRestoresIt() throws IOException {
        ClassLoader beThreadLoader = new URLClassLoader(new URL[0], null);
        Thread.currentThread().setContextClassLoader(beThreadLoader);
        try {
            RecordingScanner scanner = new RecordingScanner();
            ClassLoader pluginLoader = RecordingScanner.class.getClassLoader();

            scanner.open();
            scanner.getNextBatchMeta();
            scanner.getTableSchema();
            scanner.getStatistics();
            scanner.releaseTable();
            scanner.close();

            Assertions.assertEquals(
                    new TreeSet<>(Arrays.asList("openInternal", "getNext", "parseTableSchema",
                            "collectStatistics", "closeInternal")),
                    new TreeSet<>(scanner.observed.keySet()),
                    "an entry point stopped reaching its plugin hook");
            for (Map.Entry<String, ClassLoader> observed : scanner.observed.entrySet()) {
                Assertions.assertSame(pluginLoader, observed.getValue(),
                        observed.getKey() + " ran under the wrong context classloader");
            }
            Assertions.assertSame(beThreadLoader, Thread.currentThread().getContextClassLoader(),
                    "the caller's context classloader must be restored on the way out");
        } finally {
            Thread.currentThread().setContextClassLoader(null);
        }
    }

    /** A hook that throws must still restore the caller's classloader. */
    @Test
    void failingHookStillRestoresTheContextClassLoader() {
        ClassLoader beThreadLoader = new URLClassLoader(new URL[0], null);
        Thread.currentThread().setContextClassLoader(beThreadLoader);
        try {
            RecordingScanner scanner = new RecordingScanner();
            scanner.failOnOpen = true;
            try {
                scanner.open();
            } catch (IOException expected) {
                Assertions.assertEquals("boom", expected.getMessage());
            }
            Assertions.assertSame(beThreadLoader, Thread.currentThread().getContextClassLoader());
        } finally {
            Thread.currentThread().setContextClassLoader(null);
        }
    }

    /** End of stream is reported as address 0, and the table is gone by then. */
    @Test
    void endOfStreamReturnsZeroAndReleasesTheTable() throws IOException {
        RecordingScanner scanner = new RecordingScanner();
        scanner.open();
        Assertions.assertNotEquals(0, scanner.getNextBatchMeta());
        scanner.resetTable();
        scanner.rowsLeft = 0;
        Assertions.assertEquals(0, scanner.getNextBatchMeta());
        Assertions.assertTrue(scanner.getTable() == null, "the table must be released before EOF is reported");
        scanner.close();
    }

    private static void assertSignature(String name, Class<?> returnType, Class<?>... params)
            throws NoSuchMethodException {
        Method method = JniScanner.class.getMethod(name, params);
        Assertions.assertEquals(returnType, method.getReturnType(), name + " changed its return type");
        Assertions.assertEquals(JniScanner.class, method.getDeclaringClass(),
                name + " must be declared on JniScanner: BE resolves its method id there, not on the"
                        + " concrete plugin class");
    }

    private static final class RecordingScanner extends JniScanner {
        private final Map<String, ClassLoader> observed = new HashMap<>();
        private boolean failOnOpen;
        private int rowsLeft = 1;

        private RecordingScanner() {
            // A zero-column table, the shape BE uses for count(*): rows are counted, not materialized.
            // It keeps this test about the contract rather than about column encoding.
            initTableInfo(new ColumnType[0], new String[0], 4);
        }

        @Override
        protected void openInternal() throws IOException {
            observed.put("openInternal", Thread.currentThread().getContextClassLoader());
            if (failOnOpen) {
                throw new IOException("boom");
            }
        }

        @Override
        protected void closeInternal() {
            observed.put("closeInternal", Thread.currentThread().getContextClassLoader());
        }

        @Override
        protected int getNext() {
            observed.put("getNext", Thread.currentThread().getContextClassLoader());
            if (rowsLeft == 0) {
                return 0;
            }
            vectorTable.appendVirtualData(1);
            rowsLeft--;
            return 1;
        }

        @Override
        protected String parseTableSchema() {
            observed.put("parseTableSchema", Thread.currentThread().getContextClassLoader());
            return "{}";
        }

        @Override
        protected Map<String, String> collectStatistics() {
            observed.put("collectStatistics", Thread.currentThread().getContextClassLoader());
            return Collections.singletonMap("counter:rows", "1");
        }
    }
}
