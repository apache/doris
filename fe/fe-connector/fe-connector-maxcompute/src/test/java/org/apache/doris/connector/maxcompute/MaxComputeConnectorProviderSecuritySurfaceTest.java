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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.spi.ConnectorSecuritySurface;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;

/**
 * Pins the MaxCompute connector's secrets: the properties it marks sensitive, the access key pair. Before
 * the connector declared them, fe-core masked only {@code mc.secret_key} and SHOW CREATE CATALOG printed
 * {@code mc.access_key} in clear.
 */
public class MaxComputeConnectorProviderSecuritySurfaceTest {

    @Test
    public void declaresTheAccessKeyPair() {
        ConnectorSecuritySurface surface = new MaxComputeConnectorProvider().securitySurface();
        Assertions.assertEquals(new HashSet<>(Arrays.asList(MCCatalogProperties.ACCESS_KEY,
                MCCatalogProperties.SECRET_KEY)), new HashSet<>(surface.sensitiveKeys()));
        Assertions.assertTrue(surface.sensitiveKeyPrefixes().isEmpty());
    }
}
