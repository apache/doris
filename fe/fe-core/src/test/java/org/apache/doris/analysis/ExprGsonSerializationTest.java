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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Type;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TExprNodeType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ExprGsonSerializationTest {
    @Test
    public void testFileLiteralRoundTrip() throws Exception {
        FileLiteral literal = new FileLiteral(Arrays.asList(
                new StringLiteral("s3://bucket/gson.bin"), NullLiteral.create(Type.BIGINT),
                new IntLiteral(3L), new StringLiteral("application/octet-stream"),
                NullLiteral.create(Type.STRING), new VarBinaryLiteral("bin".getBytes(StandardCharsets.UTF_8))));
        String json = GsonUtils.GSON.toJson(literal, Expr.class);
        Expr restored = GsonUtils.GSON.fromJson(json, Expr.class);
        Assertions.assertInstanceOf(FileLiteral.class, restored);
        Assertions.assertEquals(literal, restored);
        Assertions.assertEquals(literal.toSql(), restored.toSql());
        Assertions.assertEquals(literal.getStringValue(), restored.getStringValue());
        Assertions.assertEquals(TExprNodeType.FILE_LITERAL, restored.treeToThrift().getNodes().get(0).getNodeType());
    }
}
