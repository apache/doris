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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A cache class for storing and retrieving PreExecutionAuthenticator instances based on Hadoop configurations.
 * This class caches PreExecutionAuthenticator objects to avoid recreating them for the same Hadoop configuration.
 * It uses a Least Recently Used (LRU) cache, where the least recently used entries are removed when the cache exceeds
 * the maximum size (MAX_CACHE_SIZE).
 * <p>
 * The purpose of this class is to ensure that for identical Hadoop configurations (key-value pairs),
 * only one PreExecutionAuthenticator instance is created and reused, optimizing performance by reducing
 * redundant instantiations.
 */
public class PreExecutionAuthenticatorCache {
    private static final Logger LOG = LogManager.getLogger(PreExecutionAuthenticatorCache.class);
    private static final int MAX_CACHE_SIZE = 100;

    private static final Cache<String, PreExecutionAuthenticator> preExecutionAuthenticatorCache =
            CacheBuilder.newBuilder()
                    .maximumSize(MAX_CACHE_SIZE)
                    .expireAfterAccess(60 * 24, TimeUnit.MINUTES)
                    .build();

    /**
     * Retrieves a PreExecutionAuthenticator instance from the cache or creates a new one if it doesn't exist.
     * This method first checks if the configuration is already cached. If not, it computes a new instance and
     * caches it for future use.
     *
     * @param hadoopConfig The Hadoop configuration (key-value pairs)
     * @return A PreExecutionAuthenticator instance for the given configuration
     */
    public static PreExecutionAuthenticator getAuthenticator(Map<String, String> hadoopConfig) {
        String authenticatorCacheKey = AuthenticationConfig.generalAuthenticationConfigKey(hadoopConfig);
        PreExecutionAuthenticator authenticator;
        try {
            authenticator = preExecutionAuthenticatorCache.get(authenticatorCacheKey,
                    () -> createAuthenticator(hadoopConfig, authenticatorCacheKey));
        } catch (ExecutionException exception) {
            throw new RuntimeException("Failed to create PreExecutionAuthenticator for key: " + authenticatorCacheKey,
                    exception);
        }
        return authenticator;
    }

    private static PreExecutionAuthenticator createAuthenticator(Map<String, String> hadoopConfig,
                                                                 String authenticatorCacheKey) {
        Configuration conf = new Configuration();
        hadoopConfig.forEach(conf::set);
        PreExecutionAuthenticator preExecutionAuthenticator = new PreExecutionAuthenticator();
        AuthenticationConfig authenticationConfig = AuthenticationConfig.getKerberosConfig(
                conf, AuthenticationConfig.HADOOP_KERBEROS_PRINCIPAL,
                AuthenticationConfig.HADOOP_KERBEROS_KEYTAB);
        HadoopAuthenticator hadoopAuthenticator = HadoopAuthenticator
                .getHadoopAuthenticator(authenticationConfig);
        preExecutionAuthenticator.setHadoopAuthenticator(hadoopAuthenticator);
        LOG.info("Creating new PreExecutionAuthenticator for configuration, Cache key: {}",
                authenticatorCacheKey);
        return preExecutionAuthenticator;
    }

}
