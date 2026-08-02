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

package org.apache.doris.connector.fluss;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Guards the statement-scope naming norm: every namespace this connector declares is prefixed with its
 * own connector type. The scope is keyed by {@code (catalogId, db, table, queryId)} and shared across
 * connectors, so an unprefixed namespace could hand a gateway statement another connector's value for
 * the same table coordinate — which surfaces as a ClassCastException, not as a wrong answer, but only
 * on the gateway path nobody runs by default.
 */
public class FlussStatementScopeTest {

    @Test
    public void allNamespacesArePrefixedWithConnectorType() throws Exception {
        // Reflective on purpose: a namespace added later is covered without anyone remembering to come
        // back here, and a drift in getType() turns this red on its own.
        String prefix = new FlussConnectorProvider().getType() + ".";
        int checked = 0;
        for (Field field : FlussStatementScope.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class
                    && field.getName().endsWith("_NAMESPACE")) {
                field.setAccessible(true);
                String namespace = (String) field.get(null);
                Assertions.assertTrue(namespace.startsWith(prefix),
                        field.getName() + " (\"" + namespace + "\") must start with \"" + prefix + "\"");
                checked++;
            }
        }
        Assertions.assertTrue(checked > 0, "expected at least one *_NAMESPACE constant to guard");
    }
}
