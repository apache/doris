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

import java.util.HashMap;
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

    private static final Cache<HadoopConfigWrapper, PreExecutionAuthenticator> preExecutionAuthenticatorCache =
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

        HadoopConfigWrapper hadoopConfigWrapper = new HadoopConfigWrapper(hadoopConfig);

        PreExecutionAuthenticator authenticator = null;
        try {
            authenticator = preExecutionAuthenticatorCache.get(hadoopConfigWrapper, () -> {
                Configuration conf = new Configuration();
                hadoopConfig.forEach(conf::set);
                PreExecutionAuthenticator preExecutionAuthenticator = new PreExecutionAuthenticator();
                AuthenticationConfig authenticationConfig = AuthenticationConfig.getKerberosConfig(
                        conf, AuthenticationConfig.HADOOP_KERBEROS_PRINCIPAL,
                        AuthenticationConfig.HADOOP_KERBEROS_KEYTAB);
                HadoopAuthenticator hadoopAuthenticator = HadoopAuthenticator
                        .getHadoopAuthenticator(authenticationConfig);
                preExecutionAuthenticator.setHadoopAuthenticator(hadoopAuthenticator);
                LOG.info("Created new authenticator for configuration: " + hadoopConfigWrapper);
                return preExecutionAuthenticator;
            });
        } catch (ExecutionException exception) {
            throw new RuntimeException(exception.getCause().getMessage(), exception);
        }
        return authenticator;
    }


    /**
     * Hadoop configuration wrapper class that wraps a Map<String, String> configuration.
     * This class overrides the equals() and hashCode() methods to enable comparison of
     * the configurations in the cache, ensuring that identical configurations (with the same key-value pairs)
     * are considered equal and can reuse the same cached PreExecutionAuthenticator instance.
     * <p>
     * The purpose of this class is to ensure that in the cache, if two configurations are identical
     * (i.e., they have the same key-value pairs), only one instance of PreExecutionAuthenticator is created and cached.
     * By implementing custom equals() and hashCode() methods, we ensure that even if different Map instances
     * hold the same configuration data, they are considered equal in the cache.
     */
    private static class HadoopConfigWrapper {
        private final Map<String, String> config;

        /**
         * Constructor that takes a Map<String, String> configuration.
         *
         * @param config The Hadoop configuration, typically a Map<String, String> containing configuration key-value
         *               pairs
         */
        public HadoopConfigWrapper(Map<String, String> config) {
            this.config = new HashMap<>(config);
        }

        /**
         * Checks if two HadoopConfigWrapper objects are equal.
         * Two objects are considered equal if their wrapped Map configurations are identical
         * (i.e., the key-value pairs are the same).
         *
         * @param obj The object to compare with the current object
         * @return true if the two HadoopConfigWrapper objects have the same wrapped configuration; false otherwise
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            HadoopConfigWrapper that = (HadoopConfigWrapper) obj;
            return config.equals(that.config);
        }

        /**
         * Generates a hash code based on the Hadoop configuration.
         * Objects with the same configuration will generate the same hash code, ensuring
         * that they can be correctly matched in a Map.
         *
         * @return The hash code of the Hadoop configuration
         */
        @Override
        public int hashCode() {
            return config.hashCode();
        }
    }
}
