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

package org.apache.doris.connector.trino;

import org.apache.doris.connector.spi.ConnectorSecuritySurface;
import org.apache.doris.connector.spi.ConnectorSecuritySurface.SensitiveKeyPrefix;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Pins the Trino connector's secrets: its {@code trino.*} properties go to the Trino connector unread, so the
 * credential-like ones are declared as a namespace rather than by name.
 */
public class TrinoConnectorProviderSecuritySurfaceTest {

    @Test
    public void declaresTheCredentialLikeTrinoProperties() {
        ConnectorSecuritySurface surface = new TrinoConnectorProvider().securitySurface();
        Assertions.assertTrue(surface.sensitiveKeys().isEmpty());
        Assertions.assertEquals(1, surface.sensitiveKeyPrefixes().size());
        SensitiveKeyPrefix trino = surface.sensitiveKeyPrefixes().get(0);
        Assertions.assertTrue(trino.covers("trino.connection-password"));
        Assertions.assertTrue(trino.covers("trino.hive.s3.aws-secret-key"));
        Assertions.assertTrue(trino.covers("trino.bigquery.credentials-key"));
        Assertions.assertFalse(trino.covers("trino.connector.name"));
        Assertions.assertFalse(trino.covers("trino.connection-url"));
        Assertions.assertFalse(trino.covers("trino.connection-user"));
    }
}
