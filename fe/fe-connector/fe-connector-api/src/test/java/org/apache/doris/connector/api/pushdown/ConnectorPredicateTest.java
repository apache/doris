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

package org.apache.doris.connector.api.pushdown;

import org.apache.doris.connector.api.ConnectorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Pins the neutral O5-2 write-constraint carrier {@link ConnectorPredicate}: it is a transparent holder over
 * the engine-neutral {@link ConnectorExpression}, with a nullable inner expression (the plan may yield none).
 */
public class ConnectorPredicateTest {

    @Test
    public void exposesWrappedExpression() {
        ConnectorColumnRef ref = new ConnectorColumnRef("region", ConnectorType.of("VARCHAR"));
        ConnectorPredicate predicate = new ConnectorPredicate(ref);
        Assertions.assertSame(ref, predicate.getExpression());
    }

    @Test
    public void allowsNullExpression() {
        Assertions.assertNull(new ConnectorPredicate(null).getExpression(),
                "a plan with no target-only conjunct yields a predicate with a null inner expression");
    }
}
