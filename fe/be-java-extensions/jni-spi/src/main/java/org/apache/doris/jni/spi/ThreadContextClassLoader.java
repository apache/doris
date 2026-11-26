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

import java.io.Closeable;

/**
 * Installs a context classloader for the duration of a try-with-resources block and restores the
 * previous one on exit.
 *
 * <p>It lives in the SPI because plugins need it too: any plugin code that calls into a library
 * which spawns its own threads, or that hands control to a framework doing its own reflection, has
 * to re-establish the plugin classloader the same way the SPI base classes do at the JNI boundary.
 * Trino publishes the same class in {@code io.trino.spi.classloader} for the same reason.
 */
public class ThreadContextClassLoader implements Closeable {

    private final ClassLoader originClassLoader;

    public ThreadContextClassLoader(ClassLoader contextClassLoader) {
        this.originClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(contextClassLoader);
    }

    @Override
    public void close() {
        Thread.currentThread().setContextClassLoader(originClassLoader);
    }
}
