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

import org.apache.doris.connector.api.ConnectorValidationContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link PaimonConnector#preCreateValidation} (rereview2 B-8b): a JDBC-flavor catalog
 * with a {@code driver_url} must route it through the engine's
 * {@link ConnectorValidationContext#validateAndResolveDriverPath} security gate at CREATE CATALOG,
 * before the jar is ever loaded into the FE JVM. Mirrors {@code JdbcDorisConnector.preCreateValidation}.
 *
 * <p>Offline: a hand-written {@link RecordingValidationContext} fake records each validated url and
 * can simulate a rejected url. {@link RecordingConnectorContext} supplies the (unused-by-this-path)
 * {@code ConnectorContext}.
 */
public class PaimonConnectorPreCreateValidationTest {

    private static PaimonConnector connector(Map<String, String> props) {
        return new PaimonConnector(props, new RecordingConnectorContext());
    }

    @Test
    public void validatesJdbcDriverUrl() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "jdbc");
        props.put("jdbc.driver_url", "mysql.jar");
        RecordingValidationContext ctx = new RecordingValidationContext();

        connector(props).preCreateValidation(ctx);

        // WHY (BLOCKER B-8b): a jdbc driver_url is loaded into the FE JVM (URLClassLoader); CREATE
        // CATALOG must route it through the engine's format / white-list / secure-path gate. MUTATION:
        // dropping the preCreateValidation override -> validateAndResolveDriverPath never called -> red.
        Assertions.assertEquals(Collections.singletonList("mysql.jar"), ctx.validatedDriverUrls);
    }

    @Test
    public void validatesPaimonJdbcDriverUrlAlias() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "jdbc");
        props.put("paimon.jdbc.driver_url", "mysql.jar");
        RecordingValidationContext ctx = new RecordingValidationContext();

        connector(props).preCreateValidation(ctx);

        Assertions.assertEquals(Collections.singletonList("mysql.jar"), ctx.validatedDriverUrls,
                "the paimon.jdbc.driver_url alias must also be validated");
    }

    @Test
    public void skipsValidationForNonJdbcFlavor() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "filesystem");
        props.put("jdbc.driver_url", "mysql.jar");
        RecordingValidationContext ctx = new RecordingValidationContext();

        connector(props).preCreateValidation(ctx);

        Assertions.assertTrue(ctx.validatedDriverUrls.isEmpty(),
                "non-JDBC flavors must not trigger driver-url validation");
    }

    @Test
    public void skipsValidationWhenNoDriverUrl() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "jdbc");
        RecordingValidationContext ctx = new RecordingValidationContext();

        connector(props).preCreateValidation(ctx);

        Assertions.assertTrue(ctx.validatedDriverUrls.isEmpty(),
                "a jdbc catalog without a driver_url uses the platform driver -> nothing to validate");
    }

    @Test
    public void propagatesRejectedDriverUrl() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "jdbc");
        props.put("jdbc.driver_url", "http://evil.test/x.jar");
        RecordingValidationContext ctx = new RecordingValidationContext();
        ctx.reject = true;

        // WHY (BLOCKER B-8b): a disallowed url must FAIL CREATE CATALOG — the hook throws and the
        // connector must let it propagate, not swallow it. MUTATION: catching the exception -> no
        // throw -> red.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> connector(props).preCreateValidation(ctx));
    }

    /** Hand-written {@link ConnectorValidationContext} test double (no Mockito). */
    private static final class RecordingValidationContext implements ConnectorValidationContext {
        final List<String> validatedDriverUrls = new ArrayList<>();
        boolean reject;

        @Override
        public long getCatalogId() {
            return 0;
        }

        @Override
        public String getProperty(String key) {
            return null;
        }

        @Override
        public void storeProperty(String key, String value) {
        }

        @Override
        public String validateAndResolveDriverPath(String driverUrl) throws Exception {
            validatedDriverUrls.add(driverUrl);
            if (reject) {
                throw new IllegalArgumentException("disallowed driver url: " + driverUrl);
            }
            return "file:///resolved/" + driverUrl;
        }

        @Override
        public String computeDriverChecksum(String driverUrl) {
            return "deadbeef";
        }

        @Override
        public void requestBeConnectivityTest(byte[] serializedDescriptor, int connectionTypeValue,
                String testQuery) {
        }
    }
}
