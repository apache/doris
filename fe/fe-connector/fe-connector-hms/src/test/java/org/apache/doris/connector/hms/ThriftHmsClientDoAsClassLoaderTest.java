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

package org.apache.doris.connector.hms;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link ThriftHmsClient}'s internal {@code doAs}: it MUST pin the thread-context classloader (TCCL) to
 * the plugin (child-first) classloader that loaded this client while it creates/uses the metastore client,
 * then restore the caller's TCCL.
 *
 * <p>WHY: metastore client creation runs Hadoop's {@code SecurityUtil.<clinit>}, whose internal
 * {@code new Configuration()} captures the current TCCL to reflectively load {@code DNSDomainNameResolver}. In
 * production the hive plugin bundles its own child-first hadoop copy while fe-core carries another on the
 * system classloader; a system-loader TCCL loads {@code DNSDomainNameResolver} from fe-core's copy while
 * {@code SecurityUtil}/{@code DomainNameResolver} resolve from the plugin's copy — a classloader split-brain
 * ("class DNSDomainNameResolver not DomainNameResolver") that permanently poisons {@code SecurityUtil}
 * JVM-wide. TeamCity build 991951 failed 49 hive/iceberg-on-HMS/hudi/mtmv/kerberos cases exactly this way.
 *
 * <p>The bug is invisible under a plain surefire loader (there {@code getSystemClassLoader()} and
 * {@code ThriftHmsClient.class.getClassLoader()} are the SAME object). To make it observable we reproduce the
 * production two-copy topology: the {@link DoAsTcclProbe} is invoked THROUGH an isolated child-first loader, so
 * inside it {@code ThriftHmsClient.class.getClassLoader()} is that isolated loader — distinct from the system
 * loader. A regression to {@code getSystemClassLoader()}, to no pin, or to a dropped restore is then RED. This
 * mirrors {@code OdpsClassloaderIsolationTest}'s isolation approach.
 */
public class ThriftHmsClientDoAsClassLoaderTest {

    @Test
    public void doAsPinsPluginLoaderNotSystemAndRestores() throws Exception {
        try (IsolatedChildFirstClassLoader loader = new IsolatedChildFirstClassLoader(classpathUrls())) {
            Class<?> probe = loader.loadClass("org.apache.doris.connector.hms.DoAsTcclProbe");
            Assertions.assertSame(loader, probe.getClassLoader(),
                    "sanity: the probe must be defined by the isolated child-first loader");
            Assertions.assertSame(loader, loader.loadClass(ThriftHmsClient.class.getName()).getClassLoader(),
                    "sanity: ThriftHmsClient must be defined by the isolated loader, so getClass().getClassLoader() "
                            + "differs from the system loader (that is what makes the split-brain observable)");

            Method check = probe.getMethod("check");
            String result = (String) check.invoke(null);
            Assertions.assertEquals("OK", result,
                    "doAs must pin the TCCL to the connector's own (plugin child-first) classloader — NOT the "
                            + "system classloader (the SecurityUtil split-brain root cause) — and restore the caller's "
                            + "TCCL afterwards; probe reported: " + result);
        }
    }

    private static URL[] classpathUrls() throws Exception {
        String classpath = System.getProperty("java.class.path");
        String[] entries = classpath.split(File.pathSeparator);
        List<URL> urls = new ArrayList<>(entries.length);
        for (String entry : entries) {
            if (!entry.isEmpty()) {
                urls.add(new File(entry).toURI().toURL());
            }
        }
        return urls.toArray(new URL[0]);
    }

    /**
     * Child-first loader: defines every non-JDK class from its own URLs (delegating only JDK packages to the
     * parent). This gives the probe — and the {@code ThriftHmsClient} + hadoop it touches — an isolated copy,
     * a superset of the production plugin isolation. Mirrors {@code OdpsClassloaderIsolationTest}.
     */
    private static final class IsolatedChildFirstClassLoader extends URLClassLoader {

        IsolatedChildFirstClassLoader(URL[] urls) {
            super(urls, ClassLoader.getSystemClassLoader().getParent());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) {
                    if (isJdkClass(name)) {
                        loaded = super.loadClass(name, false);
                    } else {
                        try {
                            loaded = findClass(name);
                        } catch (ClassNotFoundException notLocal) {
                            loaded = super.loadClass(name, false);
                        }
                    }
                }
                if (resolve) {
                    resolveClass(loaded);
                }
                return loaded;
            }
        }

        private static boolean isJdkClass(String name) {
            return name.startsWith("java.") || name.startsWith("javax.")
                    || name.startsWith("jdk.") || name.startsWith("sun.")
                    || name.startsWith("com.sun.") || name.startsWith("org.w3c.")
                    || name.startsWith("org.xml.");
        }
    }
}
