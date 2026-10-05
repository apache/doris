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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CastExprHashPartitionTest {

    @Test
    void timestampNsLossyCastsCannotBindRawPartitionSlots() {
        Assertions.assertFalse(new CastExpr(Type.DATETIMEV2,
                new SlotRef(Type.TIMESTAMP_NS, true), false).canHashPartition());
        Assertions.assertFalse(new CastExpr(Type.TIMESTAMPTZ,
                new SlotRef(Type.TIMESTAMP_NS, true), false).canHashPartition());
        Assertions.assertFalse(new CastExpr(Type.TIMESTAMP_NS,
                new SlotRef(Type.DATETIMEV2, true), false).canHashPartition());
        Assertions.assertFalse(new CastExpr(Type.TIMESTAMP_NS,
                new SlotRef(Type.TIMESTAMPTZ, true), false).canHashPartition());
    }

    @Test
    void existingDateFamilyAndFixedPointBehaviorIsUnchanged() {
        Assertions.assertTrue(new CastExpr(Type.DATETIMEV2,
                new SlotRef(Type.DATEV2, true), false).canHashPartition());
        Assertions.assertTrue(new CastExpr(Type.BIGINT,
                new SlotRef(Type.INT, true), false).canHashPartition());
    }
}
