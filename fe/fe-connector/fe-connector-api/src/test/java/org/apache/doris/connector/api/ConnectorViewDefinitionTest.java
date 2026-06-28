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

package org.apache.doris.connector.api;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Value-semantics tests for {@link ConnectorViewDefinition}: the neutral {sql, dialect} carrier the
 * connector returns for a flipped external view. Both fields are required (a view always has a body and a
 * dialect the body is written in), and value equality keys on both.
 */
public class ConnectorViewDefinitionTest {

    @Test
    public void exposesSqlAndDialect() {
        ConnectorViewDefinition def = new ConnectorViewDefinition("SELECT 1", "spark");
        Assertions.assertEquals("SELECT 1", def.getSql());
        Assertions.assertEquals("spark", def.getDialect());
    }

    @Test
    public void equalsAndHashCodeKeyOnBothFields() {
        ConnectorViewDefinition base = new ConnectorViewDefinition("SELECT 1", "spark");
        Assertions.assertEquals(base, new ConnectorViewDefinition("SELECT 1", "spark"));
        Assertions.assertEquals(base.hashCode(), new ConnectorViewDefinition("SELECT 1", "spark").hashCode());
        // MUTATION: an equals that ignores sql (or dialect) would collapse distinct definitions -> red.
        Assertions.assertNotEquals(base, new ConnectorViewDefinition("SELECT 2", "spark"));
        Assertions.assertNotEquals(base, new ConnectorViewDefinition("SELECT 1", "trino"));
    }

    @Test
    public void rejectsNullSqlAndDialect() {
        // WHY: a view definition with a null body or null dialect is a programming error, not a valid state;
        // fail fast at construction. MUTATION: dropping requireNonNull -> a null leaks downstream -> red.
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorViewDefinition(null, "spark"));
        Assertions.assertThrows(NullPointerException.class,
                () -> new ConnectorViewDefinition("SELECT 1", null));
    }
}
