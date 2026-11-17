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

package org.apache.doris.connector.metastore.spi;

import org.apache.doris.connector.metastore.MetaStoreProperties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.Set;

public class MetaStoreSensitiveKeysTest {
    @Test
    public void discoversNewSensitiveAliasesWithTheDefiningClassLoader() throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        try (URLClassLoader unrelatedLoader = new URLClassLoader(new URL[0], null)) {
            thread.setContextClassLoader(unrelatedLoader);
            Assertions.assertEquals(Set.of("test.metastore.opaque", "test.metastore.opaque.alias"),
                    MetaStoreProviders.sensitivePropertyKeys());
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    public static class FutureMetaStoreProvider implements MetaStoreProvider<MetaStoreProperties> {
        @Override
        public boolean supportsType(String catalogType) {
            throw new AssertionError("Sensitive key discovery must not select or bind a backend");
        }

        @Override
        public MetaStoreProperties bind(Map<String, String> properties, Map<String, String> storageHadoopConfig) {
            throw new AssertionError("Sensitive key discovery must not bind catalog properties");
        }

        @Override
        public Set<String> sensitivePropertyKeys() {
            return ConnectorPropertiesUtils.getSensitiveKeys(FutureProperties.class);
        }
    }

    public static class FutureProperties {
        @ConnectorProperty(names = {"test.metastore.opaque", "test.metastore.opaque.alias"},
                required = false, sensitive = true, description = "A future sensitive property and its alias.")
        private String opaque = "";

        @ConnectorProperty(names = {"test.metastore.ordinary"}, required = false,
                description = "An ordinary property must not become sensitive.")
        private String ordinary = "";
    }
}
