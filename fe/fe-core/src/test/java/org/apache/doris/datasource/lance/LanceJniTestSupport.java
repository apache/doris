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

package org.apache.doris.datasource.lance;

import org.junit.jupiter.api.Assumptions;

/**
 * Skips tests that need the lance-core JNI bindings on hosts where the bundled
 * {@code liblance_jni.so} cannot load (for example CI agents whose glibc is older
 * than the one the library was built against, so {@code dlopen} fails with
 * {@code version `GLIBC_2.27' not found}).
 *
 * <p>The probe initializes {@link org.lance.JniLoader}, whose static block extracts and
 * loads the bundled native library — the same load every {@link org.lance.Dataset}
 * use triggers. Probing the loader directly keeps the failure local: without the guard,
 * the first failing test poisons {@code Dataset}'s static initialization for the whole
 * JVM and every later test in the suite degrades into {@code NoClassDefFoundError}.
 */
public final class LanceJniTestSupport {

    private static final boolean JNI_BINDINGS_LOADABLE = probeJniBindingsLoadable();
    private static final boolean ARROW_C_DATA_LOADABLE = probeArrowCDataLoadable();

    private LanceJniTestSupport() {
    }

    /**
     * Assumes the lance-core JNI bindings can load, otherwise skips the current test.
     */
    public static void assumeJniBindingsLoadable() {
        Assumptions.assumeTrue(JNI_BINDINGS_LOADABLE,
                "lance-core JNI bindings cannot load on this host (native library or glibc mismatch)");
    }

    /**
     * Assumes the Arrow C Data JNI library can load, otherwise skips the current test. Tests that write
     * datasets pass Arrow data through it; some CI hosts lack the libstdc++ it needs (CXXABI_1.3.9).
     */
    public static void assumeArrowCDataLoadable() {
        Assumptions.assumeTrue(ARROW_C_DATA_LOADABLE,
                "Arrow C Data JNI library cannot load on this host (libstdc++ mismatch)");
    }

    private static boolean probeArrowCDataLoadable() {
        try {
            Object loader = Class.forName("org.apache.arrow.c.jni.JniLoader").getMethod("get").invoke(null);
            loader.getClass().getMethod("ensureLoaded").invoke(loader);
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    private static boolean probeJniBindingsLoadable() {
        try {
            Class.forName("org.lance.JniLoader");
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}
