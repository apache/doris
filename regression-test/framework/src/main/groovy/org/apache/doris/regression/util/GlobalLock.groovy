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

package org.apache.doris.regression.util

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

/** GlobalLock */
class GlobalLock {
    private static final ConcurrentHashMap<String, RefLock> locks = new ConcurrentHashMap<>()

    private static class RefLock {
        final ReentrantLock lock = new ReentrantLock()
        final AtomicInteger refCount = new AtomicInteger(0)
    }

    static void lock(String name) {
        def refLock = locks.compute(name, (k, v) -> {
            if (v == null) {
                v = new RefLock()
            }
            v.refCount.incrementAndGet()
            return v
        })
        refLock.lock.lock()
    }

    static void unlock(String name) {
        RefLock refLock = locks.get(name)
        if (refLock == null) {
            throw new IllegalMonitorStateException("Unlock called for non-existing lock: " + name)
        }

        refLock.lock.unlock()
        locks.computeIfPresent(name, (k, v) -> {
            if (v.refCount.decrementAndGet() == 0) {
                return null
            }
            return v
        })
    }
}
