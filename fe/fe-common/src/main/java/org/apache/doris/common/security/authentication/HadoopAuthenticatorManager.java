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

package org.apache.doris.common.security.authentication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

/**
 * HadoopAuthenticatorManager is a centralized manager responsible for managing and reusing instances
 * of {@link HadoopAuthenticator}. It ensures that authenticators are created and cached based on
 * the given {@link AuthenticationConfig} to minimize redundant authenticator and resource creation.
 *
 * <p>Key Features:</p>
 * <ul>
 *   <li>Uses a {@link ConcurrentHashMap} to cache authenticators and avoid creating duplicates.</li>
 *   <li>Supports different authentication mechanisms, such as Kerberos and Simple Authentication.</li>
 *   <li>Provides a unified entry point to retrieve a {@link HadoopAuthenticator} instance
 *       based on the given {@link AuthenticationConfig}.</li>
 * </ul>
 *
 * <p>How It Works:</p>
 * <ol>
 *   <li>When a new {@link AuthenticationConfig} is passed to {@code getAuthenticator},
 *       it checks if an existing authenticator is cached for the configuration.</li>
 *   <li>If an authenticator exists, it is returned directly.</li>
 *   <li>If not, a new authenticator is created using {@code createAuthenticator} and cached for future use.</li>
 * </ol>
 *
 * <p>This design helps optimize resource usage, particularly when working with file systems
 * that depend on authentication mechanisms (e.g., Hadoop's {@link org.apache.hadoop.security.UserGroupInformation}).
 * </p>
 *
 * <p>Note:</p>
 * <ul>
 *   <li>Ensure that {@link AuthenticationConfig} implementations have properly implemented
 *       {@code equals} and {@code hashCode} to guarantee correct caching behavior.</li>
 *   <li>The class is thread-safe, leveraging the thread-safety guarantees of {@link ConcurrentHashMap}.</li>
 * </ul>
 *
 * @see HadoopAuthenticator
 * @see AuthenticationConfig
 * @see KerberosAuthenticationConfig
 * @see SimpleAuthenticationConfig
 */
public class HadoopAuthenticatorManager {
    private static final Logger LOG = LogManager.getLogger(HadoopAuthenticatorManager.class);

    private static final ConcurrentHashMap<AuthenticationConfig, HadoopAuthenticator> authenticatorMap =
            new ConcurrentHashMap<>();

    public static HadoopAuthenticator getAuthenticator(AuthenticationConfig config) {
        return authenticatorMap.computeIfAbsent(config, HadoopAuthenticatorManager::createAuthenticator);
    }

    private static HadoopAuthenticator createAuthenticator(AuthenticationConfig config) {
        LOG.info("Creating a new authenticator.");
        if (config instanceof KerberosAuthenticationConfig) {
            return new HadoopKerberosAuthenticator((KerberosAuthenticationConfig) config);
        } else if (config instanceof SimpleAuthenticationConfig) {
            return new HadoopSimpleAuthenticator((SimpleAuthenticationConfig) config);
        } else {
            throw new IllegalArgumentException("Unsupported AuthenticationConfig type: " + config.getClass().getName());
        }
    }
}

