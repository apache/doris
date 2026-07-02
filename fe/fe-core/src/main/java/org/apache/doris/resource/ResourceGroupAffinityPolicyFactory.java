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

package org.apache.doris.resource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Discovers a {@link ResourceGroupAffinityPolicy} via {@link ServiceLoader}. If no custom
 * provider is on the classpath, falls back to the interface's no-op defaults.
 */
public class ResourceGroupAffinityPolicyFactory {
    private static final Logger LOG = LogManager.getLogger(ResourceGroupAffinityPolicyFactory.class);

    private static final ResourceGroupAffinityPolicy DEFAULT = new ResourceGroupAffinityPolicy() {
    };

    private static volatile ResourceGroupAffinityPolicy instance;

    public static ResourceGroupAffinityPolicy get() {
        if (instance == null) {
            synchronized (ResourceGroupAffinityPolicyFactory.class) {
                if (instance == null) {
                    instance = load(ResourceGroupAffinityPolicyFactory.class.getClassLoader());
                }
            }
        }
        return instance;
    }

    static ResourceGroupAffinityPolicy load(ClassLoader classLoader) {
        ServiceLoader<ResourceGroupAffinityPolicy> loader =
                ServiceLoader.load(ResourceGroupAffinityPolicy.class, classLoader);
        Iterator<ResourceGroupAffinityPolicy> it = loader.iterator();
        if (it.hasNext()) {
            ResourceGroupAffinityPolicy policy = it.next();
            LOG.info("Loaded ResourceGroupAffinityPolicy implementation: {}", policy.getClass().getName());
            return policy;
        }
        LOG.info("No ResourceGroupAffinityPolicy implementation found, using no-op (affinity disabled)");
        return DEFAULT;
    }
}
