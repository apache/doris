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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.ConnectorSecuritySurface;
import org.apache.doris.connector.spi.ConnectorSecuritySurface.SensitiveKeyPrefix;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Pins the ADBC connector's secrets: the password, and the driver options passed through under
 * {@code adbc.} whose names carry a credential-like word, since each ADBC driver names its own credentials.
 */
public class AdbcConnectorProviderSecuritySurfaceTest {

    @Test
    public void declaresThePasswordAndTheCredentialLikeDriverOptions() {
        ConnectorSecuritySurface surface = new AdbcConnectorProvider().securitySurface();
        Assertions.assertEquals(Collections.singletonList("password"), surface.sensitiveKeys());
        Assertions.assertEquals(1, surface.sensitiveKeyPrefixes().size());
        SensitiveKeyPrefix driverOptions = surface.sensitiveKeyPrefixes().get(0);
        // A driver option keeps its own adbc. name behind the pass-through prefix.
        Assertions.assertTrue(driverOptions.covers("adbc.adbc.flight.sql.authorization_header"));
        Assertions.assertTrue(driverOptions.covers("adbc.adbc.snowflake.sql.client_option.auth_token"));
        Assertions.assertTrue(driverOptions.covers("adbc.adbc.snowflake.sql.client_option.jwt_private_key"));
        Assertions.assertFalse(driverOptions.covers("adbc.adbc.snowflake.sql.db"));
        Assertions.assertFalse(driverOptions.covers("uri"));
        Assertions.assertTrue(surface.ssrfEndpointKeys().isEmpty());
    }
}
