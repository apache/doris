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

package org.apache.doris.catalog;

import org.apache.doris.catalog.MetaIdGenerator.IdGeneratorBuffer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class MetaIdGeneratorTest {

    @Test
    public void normalTest() {
        MetaIdGenerator idGenerator = new MetaIdGenerator(10);
        Assertions.assertEquals(10, idGenerator.getBatchEndId());
        Assertions.assertEquals(11, idGenerator.getNextId());
        Assertions.assertEquals(1010, idGenerator.getBatchEndId());

        IdGeneratorBuffer idGeneratorBuffer = idGenerator.getIdGeneratorBuffer(3500);
        Assertions.assertEquals(12, idGeneratorBuffer.getNextId());
        Assertions.assertEquals(4010, idGenerator.getBatchEndId());
        for (int i = 1; i < 3500; i++) {
            Assertions.assertEquals(i + 12, idGeneratorBuffer.getNextId());
        }
        Assertions.assertEquals(3511, idGeneratorBuffer.getBatchEndId());
        Assertions.assertEquals(3512, idGenerator.getNextId());
    }
}
