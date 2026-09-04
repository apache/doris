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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Unit tests for {@link TrinoConnectorProvider}: CREATE CATALOG must fail fast at
 * validation time when the required {@code trino.connector.name} property is absent,
 * so the error surfaces on catalog creation rather than on first query.
 */
public class TrinoConnectorProviderTest {

    private static final String NAME_PROP = "trino.connector.name";

    private final TrinoConnectorProvider provider = new TrinoConnectorProvider();

    @Test
    public void testTypeIsTrinoConnector() {
        Assertions.assertEquals("trino-connector", provider.getType());
    }

    @Test
    public void testMissingConnectorNameThrows() {
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> provider.validateProperties(Collections.emptyMap()));
        Assertions.assertTrue(ex.getMessage().contains(NAME_PROP),
                "error message should name the missing property");
    }

    @Test
    public void testEmptyConnectorNameThrows() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> provider.validateProperties(ImmutableMap.of(NAME_PROP, "")));
    }

    @Test
    public void testPresentConnectorNamePasses() {
        Assertions.assertDoesNotThrow(
                () -> provider.validateProperties(ImmutableMap.of(NAME_PROP, "postgresql")));
    }
}
