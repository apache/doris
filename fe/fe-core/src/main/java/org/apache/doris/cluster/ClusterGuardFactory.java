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

package org.apache.doris.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Factory that discovers a {@link ClusterGuard} implementation via
 * {@link ServiceLoader}.
 * <p>
 * If no provider is found on the classpath and the sentinel resource
 * {@code META-INF/cluster-guard-required} is absent, a {@link NoOpClusterGuard}
 * is returned so the open-source edition runs without restrictions.
 * <p>
 * When the sentinel resource <em>is</em> present but no implementation is found,
 * startup is aborted by throwing a {@link RuntimeException}. The sentinel is
 * only shipped by builds that require guard enforcement; it is never included
 * in the open-source distribution.
 */
public class ClusterGuardFactory {
    private static final Logger LOG = LogManager.getLogger(ClusterGuardFactory.class);

    static final String SENTINEL_RESOURCE = "META-INF/cluster-guard-required";

    private static volatile ClusterGuard instance;

    /**
     * Get the singleton ClusterGuard instance.
     * On first call, discovers the implementation via ServiceLoader.
     */
    public static ClusterGuard getGuard() {
        if (instance == null) {
            synchronized (ClusterGuardFactory.class) {
                if (instance == null) {
                    instance = loadGuard();
                }
            }
        }
        return instance;
    }

    private static ClusterGuard loadGuard() {
        return loadGuard(ClusterGuardFactory.class.getClassLoader());
    }

    /**
     * Visible for testing — allows injecting a custom {@link ClassLoader} so unit
     * tests can simulate the sentinel file being present or absent without touching
     * the real classpath.
     */
    static ClusterGuard loadGuard(ClassLoader classLoader) {
        ServiceLoader<ClusterGuard> loader = ServiceLoader.load(ClusterGuard.class, classLoader);
        Iterator<ClusterGuard> it = loader.iterator();
        if (it.hasNext()) {
            ClusterGuard guard = it.next();
            LOG.info("Loaded ClusterGuard implementation: {}", guard.getClass().getName());
            return guard;
        }

        if (classLoader.getResource(SENTINEL_RESOURCE) != null) {
            throw new RuntimeException(
                    "ClusterGuard is required but no implementation was found on the classpath. "
                    + "Ensure the appropriate extension module is included in the deployment.");
        }

        LOG.info("No ClusterGuard implementation found, using NoOpClusterGuard");
        return NoOpClusterGuard.INSTANCE;
    }
}
