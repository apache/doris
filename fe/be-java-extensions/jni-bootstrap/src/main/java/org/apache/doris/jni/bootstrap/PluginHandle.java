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

import org.apache.doris.jni.spi.JniScannerFactory;
import org.apache.doris.jni.spi.JniWriterFactory;
import org.apache.doris.jni.spi.UdfExecutorFactory;

import java.util.Collections;
import java.util.Map;

/**
 * The outcome of trying to load one plugin, cached for the life of the process.
 *
 * <p>Failure is a state, not an exception thrown out of the runtime: a plugin that cannot load must
 * cost exactly the queries that use it. The three states are what BE can report:
 *
 * <ul>
 *   <li>{@code NOT_DEPLOYED} - the directory is absent. Expected on a deployment that does not use
 *       this connector, so it is not logged as an error.</li>
 *   <li>{@code FAILED} - the directory is there but something in it is wrong. The cause is kept and
 *       replayed on every request, so the tenth query gets the same message as the first.</li>
 *   <li>{@code READY} - the plugin's factories are indexed by name.</li>
 * </ul>
 */
final class PluginHandle {

    enum State {
        NOT_DEPLOYED,
        FAILED,
        READY
    }

    private final String name;
    private final State state;
    private final String failure;
    private final String declaredApiVersion;
    private final DorisPluginClassLoader classLoader;
    private final Map<String, JniScannerFactory> scannerFactories;
    private final Map<String, JniWriterFactory> writerFactories;
    private final Map<String, UdfExecutorFactory> udfExecutorFactories;

    private PluginHandle(String name, State state, String failure, String declaredApiVersion,
            DorisPluginClassLoader classLoader,
            Map<String, JniScannerFactory> scannerFactories,
            Map<String, JniWriterFactory> writerFactories,
            Map<String, UdfExecutorFactory> udfExecutorFactories) {
        this.name = name;
        this.state = state;
        this.failure = failure;
        this.declaredApiVersion = declaredApiVersion;
        this.classLoader = classLoader;
        this.scannerFactories = scannerFactories;
        this.writerFactories = writerFactories;
        this.udfExecutorFactories = udfExecutorFactories;
    }

    static PluginHandle notDeployed(String name) {
        return new PluginHandle(name, State.NOT_DEPLOYED, null, null, null,
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    }

    static PluginHandle failed(String name, String failure) {
        return new PluginHandle(name, State.FAILED, failure, null, null,
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    }

    static PluginHandle ready(String name, String declaredApiVersion, DorisPluginClassLoader classLoader,
            Map<String, JniScannerFactory> scannerFactories,
            Map<String, JniWriterFactory> writerFactories,
            Map<String, UdfExecutorFactory> udfExecutorFactories) {
        return new PluginHandle(name, State.READY, null, declaredApiVersion, classLoader,
                Collections.unmodifiableMap(scannerFactories),
                Collections.unmodifiableMap(writerFactories),
                Collections.unmodifiableMap(udfExecutorFactories));
    }

    String name() {
        return name;
    }

    State state() {
        return state;
    }

    String failure() {
        return failure;
    }

    String declaredApiVersion() {
        return declaredApiVersion;
    }

    DorisPluginClassLoader classLoader() {
        return classLoader;
    }

    Map<String, JniScannerFactory> scannerFactories() {
        return scannerFactories;
    }

    Map<String, JniWriterFactory> writerFactories() {
        return writerFactories;
    }

    Map<String, UdfExecutorFactory> udfExecutorFactories() {
        return udfExecutorFactories;
    }
}
