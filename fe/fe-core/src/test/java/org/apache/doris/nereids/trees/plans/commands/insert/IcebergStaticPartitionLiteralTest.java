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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class IcebergStaticPartitionLiteralTest {
    @Test
    public void preserveNullSeparatelyFromTextAndEmptyBytes() throws Exception {
        Map<String, Expression> literals = ImmutableMap.of(
                "null_value", NullLiteral.INSTANCE,
                "text_value", new StringLiteral("null"),
                "empty_bytes", new VarBinaryLiteral(new byte[0]),
                "raw_bytes", new VarBinaryLiteral(new byte[] {0, (byte) 0xFF, (byte) 0x80}));
        Map<String, String> result = InsertOverwriteTableCommand.encodeStaticPartitionSpec(literals);
        Assertions.assertTrue(result.containsKey("null_value"));
        Assertions.assertNull(result.get("null_value"));
        Assertions.assertEquals("null", result.get("text_value"));
        Assertions.assertEquals("0x", result.get("empty_bytes"));
        Assertions.assertEquals("0x00FF80", result.get("raw_bytes"));
    }
}
