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

package org.apache.doris.udf;

import java.util.Objects;

/**
 * The parent classloader of every user function, exposing the JDK plus exactly two things out of
 * this plugin.
 *
 * <h2>Why a user function does not simply see the plugin</h2>
 *
 * <p>A user function is code Doris did not write, and it should depend on nothing Doris happens to
 * ship. Before plugins, it was loaded against BE's whole system classpath and could reach guava,
 * jackson, netty, hadoop and everything else two fat jars dragged along - not by design, but
 * because they were there. Every one of those became a compatibility surface nobody chose, and the
 * versions on it changed whenever an unrelated dependency did.
 *
 * <p>So the rule is that a user function ships what it uses. What it cannot ship is the handful of
 * classes that have to be <em>the same class object</em> on both sides:
 *
 * <ul>
 *   <li>{@code org.joda.time.LocalDate}, {@code DateTime} and {@code LocalDateTime}, because
 *       {@code Type.DATE_SUPPORTED_JAVA_TYPE} and its siblings are sets of {@code Class} objects
 *       and a function's declared parameter type is matched against them by identity. A second
 *       copy of joda does not match, and the function is rejected as having no usable
 *       {@code evaluate}.</li>
 *   <li>{@code org.apache.hadoop.hive.ql.exec.UDF} and the classes that load with it, which a user
 *       function may extend. Nothing in this plugin refers to them; they are here so that such a
 *       function links.</li>
 * </ul>
 *
 * <p>Everything else resolves against the JDK or against the user's own jar, and a name that is in
 * neither fails here rather than binding to whatever BE happens to have.
 */
public final class UdfContractClassLoader extends ClassLoader {

    /**
     * The only names delegated into the plugin. Prefixes rather than exact names because the Hive
     * UDF base class drags a small closure with it and joda's date classes reference the rest of
     * joda; listing the leaves would break on the first library upgrade.
     */
    private static final String[] CONTRACT_PREFIXES = {
            "org.apache.hadoop.hive.ql.exec.",
            "org.apache.hadoop.hive.ql.metadata.",
            "org.joda.time.",
    };

    static {
        registerAsParallelCapable();
    }

    private final ClassLoader pluginClassLoader;

    /**
     * @param pluginClassLoader this plugin's own loader, the only place contract classes come from
     */
    public UdfContractClassLoader(ClassLoader pluginClassLoader) {
        // The plugin's loader has the platform loader as its parent - DorisPluginClassLoader is
        // built that way so that no plugin can reach BE's system classpath. Taking it from there
        // rather than naming it keeps this module compilable at Java 8 and keeps the two loaders
        // from disagreeing about what "the JDK" means.
        super(parentOf(pluginClassLoader));
        this.pluginClassLoader = pluginClassLoader;
    }

    private static ClassLoader parentOf(ClassLoader pluginClassLoader) {
        Objects.requireNonNull(pluginClassLoader, "pluginClassLoader");
        ClassLoader platform = pluginClassLoader.getParent();
        if (platform == null) {
            // A null parent is the bootstrap loader, which would leave user functions unable to
            // resolve java.sql and the rest of the platform modules. It can only happen if this
            // plugin was loaded by something other than DorisPluginClassLoader.
            throw new IllegalStateException("The java-udf plugin was loaded by a classloader with no"
                    + " parent (" + pluginClassLoader + "), so there is no platform classloader to put"
                    + " behind user functions.");
        }
        return platform;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            if (isContractClass(name)) {
                Class<?> contract = pluginClassLoader.loadClass(name);
                if (resolve) {
                    resolveClass(contract);
                }
                return contract;
            }
            // Platform only: this loader owns no classes of its own, so anything that is not in
            // the JDK and not in the user's jar ends here. The failure is left bare because the
            // caller is always a user function's own loader, which discards whatever its parent
            // threw and reports its own miss; the explanation lives there, in
            // UserFunctionClassLoader.
            return super.loadClass(name, resolve);
        }
    }

    static boolean isContractClass(String name) {
        for (String prefix : CONTRACT_PREFIXES) {
            if (name.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}
