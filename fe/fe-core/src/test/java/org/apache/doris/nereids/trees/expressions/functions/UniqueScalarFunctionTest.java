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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.functions.scalar.RandomBytes;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Uuid;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UuidNumeric;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class UniqueScalarFunctionTest {

    @Test
    void testEquals() {
        Random rand0 = new Random();
        Random rand1 = new Random(new BigIntLiteral(10L));
        Random rand2 = new Random(new BigIntLiteral(1L), new BigIntLiteral(10L));
        Assertions.assertNotEquals(rand0, new Random());
        Assertions.assertEquals(rand0.withIgnoreUniqueId(true), new Random().withIgnoreUniqueId(true));
        Assertions.assertEquals(rand0, rand0.withChildren());
        Assertions.assertEquals(rand0, rand0.withChildren(new BigIntLiteral(10L))); // only compare unique id
        Assertions.assertNotEquals(rand1, new Random(new BigIntLiteral(10L)));
        Assertions.assertEquals(rand1.withIgnoreUniqueId(true), new Random(new BigIntLiteral(10L)).withIgnoreUniqueId(true));
        Assertions.assertEquals(rand1, rand1.withChildren(new BigIntLiteral(10L)));
        Assertions.assertEquals(rand1,
                rand1.withChildren(new BigIntLiteral(1L), new BigIntLiteral(10L))); // only compare unique id
        Assertions.assertNotEquals(rand2, new Random(new BigIntLiteral(1L), new BigIntLiteral(10L)));
        Assertions.assertEquals(rand2.withIgnoreUniqueId(true), new Random(new BigIntLiteral(1L), new BigIntLiteral(10L)).withIgnoreUniqueId(true));
        Assertions.assertEquals(rand2, rand2.withChildren(new BigIntLiteral(1L), new BigIntLiteral(10L)));

        RandomBytes randb = new RandomBytes(new BigIntLiteral(10L));
        Assertions.assertNotEquals(randb, new RandomBytes(new BigIntLiteral(10L)));
        Assertions.assertEquals(randb.withIgnoreUniqueId(true), new RandomBytes(new BigIntLiteral(10L)).withIgnoreUniqueId(true));
        Assertions.assertEquals(randb, randb.withChildren(new BigIntLiteral(10L)));
        Assertions.assertEquals(randb, randb.withChildren(new BigIntLiteral(1L))); // only compare unique id

        Uuid uuid = new Uuid();
        Assertions.assertNotEquals(uuid, new Uuid());
        Assertions.assertEquals(uuid.withIgnoreUniqueId(true), new Uuid().withIgnoreUniqueId(true));
        Assertions.assertEquals(uuid, uuid.withChildren());

        UuidNumeric uuidNum = new UuidNumeric();
        Assertions.assertNotEquals(uuidNum, new UuidNumeric());
        Assertions.assertEquals(uuidNum.withIgnoreUniqueId(true), new UuidNumeric().withIgnoreUniqueId(true));
        Assertions.assertEquals(uuidNum, uuidNum.withChildren());
    }
}
