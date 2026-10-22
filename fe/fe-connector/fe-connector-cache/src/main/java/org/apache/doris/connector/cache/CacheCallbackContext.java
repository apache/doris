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

package org.apache.doris.connector.cache;

import java.util.function.Supplier;

/** Restores the registering plugin's loader around callbacks, including asynchronous cleanup. */
public final class CacheCallbackContext {
    private final ClassLoader loader;

    public CacheCallbackContext(ClassLoader loader) {
        this.loader = loader;
    }

    public <T> T call(Supplier<T> callback) {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        if (previous == loader) {
            return callback.get();
        }
        try {
            thread.setContextClassLoader(loader);
            return callback.get();
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    public void run(Runnable callback) {
        call(() -> {
            callback.run();
            return null;
        });
    }
}
