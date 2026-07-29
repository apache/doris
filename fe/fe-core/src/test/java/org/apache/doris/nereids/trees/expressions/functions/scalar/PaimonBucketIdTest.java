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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.IntegerType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class PaimonBucketIdTest {

    @Test
    public void testSignatureAndName() {
        PaimonBucketId f = new PaimonBucketId(new IntegerLiteral(1), new IntegerLiteral(2));
        List<FunctionSignature> sigs = f.getSignatures();
        Assertions.assertEquals(1, sigs.size());
        Assertions.assertEquals(IntegerType.INSTANCE, sigs.get(0).returnType);
        Assertions.assertEquals("paimon_bucket_id", f.getName());
    }

    @Test
    public void testWithChildrenVarArgs() {
        PaimonBucketId f = new PaimonBucketId(new IntegerLiteral(1), new IntegerLiteral(2), new IntegerLiteral(3));
        List<Expression> children = f.getArguments();
        Assertions.assertEquals(3, children.size());
        PaimonBucketId g = f.withChildren(children);
        Assertions.assertEquals("paimon_bucket_id", g.getName());
        Assertions.assertEquals(3, g.getArguments().size());
    }
}
