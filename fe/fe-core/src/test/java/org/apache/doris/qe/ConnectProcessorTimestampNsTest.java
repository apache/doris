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

package org.apache.doris.qe;

import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.TimeStampNsLiteral;
import org.apache.doris.catalog.Type;
import org.apache.doris.thrift.TExprNode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

class ConnectProcessorTimestampNsTest {
    @Test
    void testForwardedTimestampNsKeepsEncodedTypeAndText() throws Exception {
        TimeStampNsLiteral[] literals = {
                new TimeStampNsLiteral(2024, 1, 2, 0, 0, 0, 0),
                new TimeStampNsLiteral(2024, 1, 2, 3, 4, 5, 123456000)
        };
        Method deserialize = ConnectProcessor.class.getDeclaredMethod(
                "getLiteralExprFromThrift", TExprNode.class);
        deserialize.setAccessible(true);

        for (TimeStampNsLiteral literal : literals) {
            TExprNode node = ExprToThriftVisitor.treeToThrift(literal).getNodes().get(0);
            LiteralExpr restored = (LiteralExpr) deserialize.invoke(null, node);

            Assertions.assertInstanceOf(TimeStampNsLiteral.class, restored);
            Assertions.assertEquals(Type.TIMESTAMP_NS, restored.getType());
            Assertions.assertEquals(literal.getStringValue(), restored.getStringValue());
        }
    }
}
