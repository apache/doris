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

package org.apache.doris.connector.fake;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.util.Collections;
import java.util.Map;

/**
 * "Empty" connector plugin used as a baseline by P0 batch-2 tests.
 *
 * <p>Implements only the bare minimum of the SPI surface so that every
 * other method on {@link Connector}, {@link ConnectorMetadata},
 * {@link ConnectorSession}, and {@link ConnectorContext} exercises its
 * default implementation. Tests that depend on a particular default
 * behavior (e.g. {@code listPartitionNames()} returning an empty list,
 * {@code beginTransaction()} throwing) can construct a fake catalog from
 * this plugin without having to stub each interface by hand.
 *
 * <p>NOT registered via {@code META-INF/services} — tests instantiate it
 * directly to keep production discovery deterministic.
 */
public final class FakeConnectorPlugin implements ConnectorProvider {

    public static final String TYPE = "fake";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        return new FakeConnector();
    }

    /** Connector exposing a metadata that overrides nothing. */
    public static final class FakeConnector implements Connector {
        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return new FakeMetadata();
        }
    }

    /** {@link ConnectorMetadata} with zero overrides — every method uses the default. */
    public static final class FakeMetadata implements ConnectorMetadata {
    }

    /** {@link ConnectorSession} that only fills the always-required fields. */
    public static final class FakeSession implements ConnectorSession {

        private final String catalogName;
        private final long catalogId;

        public FakeSession(String catalogName, long catalogId) {
            this.catalogName = catalogName;
            this.catalogId = catalogId;
        }

        @Override
        public String getQueryId() {
            return "fake-query";
        }

        @Override
        public String getUser() {
            return "fake-user";
        }

        @Override
        public String getTimeZone() {
            return "UTC";
        }

        @Override
        public String getLocale() {
            return "en_US";
        }

        @Override
        public long getCatalogId() {
            return catalogId;
        }

        @Override
        public String getCatalogName() {
            return catalogName;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T getProperty(String name, Class<T> type) {
            return null;
        }

        @Override
        public Map<String, String> getCatalogProperties() {
            return Collections.emptyMap();
        }
    }

    /** {@link ConnectorContext} that only fills catalog name + id. */
    public static final class FakeContext implements ConnectorContext {

        private final String catalogName;
        private final long catalogId;

        public FakeContext(String catalogName, long catalogId) {
            this.catalogName = catalogName;
            this.catalogId = catalogId;
        }

        @Override
        public String getCatalogName() {
            return catalogName;
        }

        @Override
        public long getCatalogId() {
            return catalogId;
        }
    }
}
