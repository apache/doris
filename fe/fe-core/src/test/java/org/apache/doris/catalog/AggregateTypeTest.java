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

import org.apache.doris.thrift.TAggregationType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AggregateTypeTest {

    @Test
    public void testToThriftAndFromName() {
        Assertions.assertEquals(TAggregationType.FIRST, AggregateType.FIRST.toThrift());
        Assertions.assertEquals(AggregateType.FIRST, AggregateType.getAggTypeFromAggName("FIRST"));
        Assertions.assertEquals(AggregateType.REPLACE, AggregateType.getAggTypeFromAggName("REPLACE"));
        Assertions.assertEquals(AggregateType.REPLACE_IF_NOT_NULL,
                AggregateType.getAggTypeFromAggName("REPLACE_IF_NOT_NULL"));
    }

    @Test
    public void testIsReplaceFamily() {
        Assertions.assertTrue(AggregateType.FIRST.isReplaceFamily());
        Assertions.assertTrue(AggregateType.REPLACE.isReplaceFamily());
        Assertions.assertTrue(AggregateType.REPLACE_IF_NOT_NULL.isReplaceFamily());
        Assertions.assertFalse(AggregateType.SUM.isReplaceFamily());
        Assertions.assertFalse(AggregateType.MIN.isReplaceFamily());
        Assertions.assertFalse(AggregateType.MAX.isReplaceFamily());
    }
}

