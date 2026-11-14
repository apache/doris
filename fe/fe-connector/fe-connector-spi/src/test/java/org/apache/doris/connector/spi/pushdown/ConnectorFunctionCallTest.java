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

package org.apache.doris.connector.spi.pushdown;

import org.apache.doris.connector.spi.ConnectorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class ConnectorFunctionCallTest {
    private static final ConnectorType BOOLEAN = ConnectorType.of("BOOLEAN");
    private static final List<ConnectorExpression> ARGS = Collections.singletonList(ConnectorLiteral.ofString("a"));

    @Test
    public void callIsBuiltinUnlessCreatedAsUdf() {
        Assertions.assertTrue(new ConnectorFunctionCall("starts_with", BOOLEAN, ARGS).isBuiltin());
        Assertions.assertTrue(new ConnectorFunctionCall("starts_with", BOOLEAN, ARGS, true).isBuiltin());
        Assertions.assertFalse(new ConnectorFunctionCall("starts_with", BOOLEAN, ARGS, false).isBuiltin());
    }

    @Test
    public void udfDiffersFromTheBuiltinOfTheSameName() {
        ConnectorFunctionCall builtin = new ConnectorFunctionCall("starts_with", BOOLEAN, ARGS);
        ConnectorFunctionCall alsoBuiltin = new ConnectorFunctionCall("starts_with", BOOLEAN, ARGS, true);
        Assertions.assertEquals(builtin, alsoBuiltin);
        Assertions.assertEquals(builtin.hashCode(), alsoBuiltin.hashCode());
        Assertions.assertNotEquals(builtin, new ConnectorFunctionCall("starts_with", BOOLEAN, ARGS, false));
    }
}
