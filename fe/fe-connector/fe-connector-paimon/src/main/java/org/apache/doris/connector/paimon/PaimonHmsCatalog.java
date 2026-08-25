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

package org.apache.doris.connector.paimon;

import org.apache.doris.kerberos.HadoopAuthenticator;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.DelegateCatalog;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

/** Keeps Paimon catalogs rebuilt by a retained loader inside the HMS authentication boundary. */
final class PaimonHmsCatalog extends DelegateCatalog {

    private final Map<String, String> properties;
    private final Map<String, String> storageHadoopConfig;

    private PaimonHmsCatalog(Catalog wrapped, Map<String, String> properties,
            Map<String, String> storageHadoopConfig) {
        super(wrapped);
        this.properties = new HashMap<>(properties);
        this.storageHadoopConfig = new HashMap<>(storageHadoopConfig);
    }

    static Catalog install(Catalog catalog, Map<String, String> properties,
            Map<String, String> storageHadoopConfig) {
        return new PaimonHmsCatalog(catalog, properties, storageHadoopConfig);
    }

    @Override
    public CatalogLoader catalogLoader() {
        return new AuthenticatedLoader(wrapped.catalogLoader(), properties, storageHadoopConfig);
    }

    private static final class AuthenticatedLoader implements CatalogLoader {
        private static final long serialVersionUID = 1L;

        private final CatalogLoader delegate;
        private final Map<String, String> properties;
        private final Map<String, String> storageHadoopConfig;

        private AuthenticatedLoader(CatalogLoader delegate, Map<String, String> properties,
                Map<String, String> storageHadoopConfig) {
            this.delegate = delegate;
            this.properties = new HashMap<>(properties);
            this.storageHadoopConfig = new HashMap<>(storageHadoopConfig);
        }

        @Override
        public Catalog load() {
            HadoopAuthenticator authenticator =
                    PaimonConnector.buildHmsAuthenticator(properties, storageHadoopConfig);
            try {
                // HiveCatalogLoader constructs a fresh eager client pool, so authentication must precede load().
                Catalog loaded = authenticator == null
                        ? delegate.load() : authenticator.doAs(delegate::load);
                loaded = PaimonHmsClientPool.install(loaded, authenticator);
                return new PaimonHmsCatalog(loaded, properties, storageHadoopConfig);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to load a Paimon HMS catalog", e);
            }
        }
    }
}
