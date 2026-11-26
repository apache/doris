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

package org.apache.doris.connector.es;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Guards that every ES statement-scope namespace follows the source-prefix norm: it is a compile-time constant
 * prefixed with this connector's {@code ConnectorProvider.getType()} ("es"). Source-prefixing is what keeps the
 * namespaces distinct from every other connector's on a heterogeneous gateway (no {@code ClassCastException} on the
 * shared {@code (catalogId, db, table, queryId)} coordinate); the value-type home is
 * {@link org.apache.doris.connector.spi.ConnectorStatementScopes}.
 */
public class EsStatementScopeTest {

    @Test
    public void allNamespacesArePrefixedWithConnectorType() throws Exception {
        // NORM (self-extending): reflect over every "*_NAMESPACE" constant this connector declares and assert each
        // is prefixed with the connector's getType() ("es."). Reflecting means a NEW namespace is auto-covered; a
        // forgotten prefix or a getType() drift turns this red with no test upkeep.
        String prefix = new EsConnectorProvider().getType() + ".";
        int checked = 0;
        for (Field f : EsStatementScope.class.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers()) && f.getType() == String.class
                    && f.getName().endsWith("_NAMESPACE")) {
                f.setAccessible(true);
                String ns = (String) f.get(null);
                Assertions.assertTrue(ns.startsWith(prefix),
                        f.getName() + " (\"" + ns + "\") must be prefixed with the connector type \"" + prefix + "\"");
                checked++;
            }
        }
        Assertions.assertTrue(checked > 0, "expected at least one *_NAMESPACE constant to guard");
    }
}
