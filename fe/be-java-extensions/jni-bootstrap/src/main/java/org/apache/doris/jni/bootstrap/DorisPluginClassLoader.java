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

package org.apache.doris.jni.bootstrap;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;

/**
 * Classloader of one plugin. It splits the class space into three parts, and which part a name
 * falls into is decided by the name alone:
 *
 * <ol>
 *   <li><b>JDK</b> - the parent is the <em>platform</em> classloader, not the system one. This is
 *       the load-bearing choice: BE's system classpath (conf, the SPI jars, and the hadoop drop
 *       that C++ libhdfs needs) is not reachable from a plugin at all.</li>
 *   <li><b>SPI</b> - names under {@code org.apache.doris.jni.spi.} are resolved only through BE's
 *       own classloader, so BE and the plugin hold the same class identity for the types they
 *       exchange.</li>
 *   <li><b>Plugin</b> - everything else comes from the plugin's own directory.</li>
 * </ol>
 *
 * <p>The point of (1) is that isolation is structural rather than a matter of search order. A
 * child-first loader that falls back to its parent still lets a class the plugin failed to package
 * resolve against whatever BE happens to ship, which is how two versions of hadoop end up mixed
 * inside one plugin with nothing reporting it. Here the same mistake is a
 * {@link ClassNotFoundException} naming the class, and the build-time jdeps gate catches it before
 * that.
 *
 * <p>Modelled on Trino's {@code io.trino.server.PluginClassLoader}, including the two diagnostics
 * below, which exist because "SPI class not found" has exactly two causes and they need different
 * fixes.
 */
public class DorisPluginClassLoader extends URLClassLoader {

    /** Prefix delegated to BE's classloader. Also the reason SPI classes live in that package. */
    static final String SPI_PACKAGE = "org.apache.doris.jni.spi.";

    /** Resource form of {@link #SPI_PACKAGE}. */
    static final String SPI_RESOURCE_PREFIX = "org/apache/doris/jni/spi/";

    static {
        registerAsParallelCapable();
    }

    private final String pluginName;
    private final ClassLoader spiClassLoader;

    /**
     * @param pluginName     the plugin's directory name, used in diagnostics
     * @param urls           the jars in that directory
     * @param spiClassLoader BE's own classloader, the only place SPI classes may come from
     */
    public DorisPluginClassLoader(String pluginName, List<URL> urls, ClassLoader spiClassLoader) {
        // Plugins must not see the system (application) classloader.
        super(urls.toArray(new URL[0]), getPlatformClassLoader());
        this.pluginName = Objects.requireNonNull(pluginName, "pluginName");
        this.spiClassLoader = Objects.requireNonNull(spiClassLoader, "spiClassLoader");
    }

    public String getPluginName() {
        return pluginName;
    }

    /**
     * True when this plugin's own jars contain SPI classes.
     *
     * <p>Checked once when the plugin is loaded rather than waiting for a confusing failure later.
     * A plugin that ships its own copy of the SPI produces classes that are not the ones BE holds,
     * so the first hand-off across the boundary fails with a ClassCastException between two
     * identically named types. {@link #findResource} deliberately searches only this loader's
     * URLs, never the parent, so a hit here means the class is physically inside the plugin.
     */
    boolean packagesSpiClasses() {
        return findResource(SPI_RESOURCE_PREFIX + "DorisPlugin.class") != null;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> cached = findLoadedClass(name);
            if (cached != null) {
                return resolved(cached, resolve);
            }

            if (isSpiClass(name)) {
                try {
                    return resolved(spiClassLoader.loadClass(name), resolve);
                } catch (ClassNotFoundException e) {
                    if (hasClassLocally(name)) {
                        throw new ClassNotFoundException("SPI class '" + name + "' was not found in BE's"
                                + " classloader, but was found inside plugin '" + pluginName + "'. The"
                                + " dependency providing this class must be declared with <scope>provided"
                                + "</scope> so that BE and the plugin share one copy.", e);
                    }
                    throw new ClassNotFoundException("SPI class '" + name + "' was not found in BE's"
                            + " classloader. This is probably a bug in the dependencies: the class is in"
                            + " the SPI package but is not part of the deployed jni-spi jar.", e);
                }
            }

            // Platform (JDK) first, then this plugin's own jars.
            return super.loadClass(name, resolve);
        }
    }

    @Override
    public URL getResource(String name) {
        if (isSpiResource(name)) {
            return spiClassLoader.getResource(name);
        }
        return super.getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        if (isSpiResource(name)) {
            return spiClassLoader.getResources(name);
        }
        // Service descriptors are resources, so a plugin's META-INF/services entries are found
        // here, locally, exactly as intended.
        return super.getResources(name);
    }

    @Override
    public String toString() {
        return "DorisPluginClassLoader{plugin=" + pluginName
                + ", identityHash=@" + Integer.toHexString(System.identityHashCode(this)) + '}';
    }

    private Class<?> resolved(Class<?> clazz, boolean resolve) {
        if (resolve) {
            resolveClass(clazz);
        }
        return clazz;
    }

    private boolean hasClassLocally(String name) {
        return findResource(name.replace('.', '/') + ".class") != null;
    }

    private static boolean isSpiClass(String name) {
        return name.startsWith(SPI_PACKAGE);
    }

    private static boolean isSpiResource(String name) {
        // Resource names are slash-separated; testing them against the dotted package prefix would
        // never match and would silently disable this delegation.
        return name.startsWith(SPI_RESOURCE_PREFIX) || name.startsWith("/" + SPI_RESOURCE_PREFIX);
    }
}
