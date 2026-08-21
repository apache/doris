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

import java.util.Collections;

/**
 * Entry point of a BE Java plugin, discovered through {@link java.util.ServiceLoader} from
 * {@code META-INF/services/org.apache.doris.jni.spi.DorisPlugin} inside the plugin's own jars.
 *
 * <p>A plugin is a directory under {@code plugins/jni/}; the directory name is the plugin
 * name BE uses to address it, so the interface deliberately carries no name of its own — there is
 * no way for a jar to disagree with the directory it was deployed into.
 *
 * <p>Every method has a default returning nothing, so a plugin declares only the kinds of
 * factories it actually provides, and adding a new kind later does not break plugins already
 * written. This is the same evolution rule Trino applies to {@code io.trino.spi.Plugin}.
 */
public interface DorisPlugin {

    /** Scanners this plugin provides, keyed by {@link JniScannerFactory#getName()}. */
    default Iterable<JniScannerFactory> getScannerFactories() {
        return Collections.emptyList();
    }

    /** Writers this plugin provides, keyed by {@link JniWriterFactory#getName()}. */
    default Iterable<JniWriterFactory> getWriterFactories() {
        return Collections.emptyList();
    }

    /** UDF executors this plugin provides, keyed by {@link UdfExecutorFactory#getName()}. */
    default Iterable<UdfExecutorFactory> getUdfExecutorFactories() {
        return Collections.emptyList();
    }
}
