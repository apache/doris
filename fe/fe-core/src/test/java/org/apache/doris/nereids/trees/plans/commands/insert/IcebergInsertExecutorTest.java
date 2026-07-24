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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

class IcebergInsertExecutorTest {

    @Test
    void testShouldExecuteEmptyInsertForIcebergOverwrite() {
        IcebergInsertCommandContext appendCtx = new IcebergInsertCommandContext();
        IcebergInsertCommandContext overwriteCtx = new IcebergInsertCommandContext();
        overwriteCtx.setOverwrite(true);
        IcebergInsertCommandContext staticOverwriteCtx = new IcebergInsertCommandContext();
        staticOverwriteCtx.setOverwrite(true);
        staticOverwriteCtx.setStaticPartitionValues(Collections.singletonMap("dt", "2026-07-07"));

        Assertions.assertFalse(IcebergInsertExecutor.shouldExecuteEmptyInsert(
                false, Optional.of(staticOverwriteCtx), true));
        Assertions.assertFalse(IcebergInsertExecutor.shouldExecuteEmptyInsert(
                true, Optional.empty(), false));
        Assertions.assertFalse(IcebergInsertExecutor.shouldExecuteEmptyInsert(
                true, Optional.of(appendCtx), false));
        Assertions.assertFalse(IcebergInsertExecutor.shouldExecuteEmptyInsert(
                true, Optional.of(overwriteCtx), true));
        Assertions.assertTrue(IcebergInsertExecutor.shouldExecuteEmptyInsert(
                true, Optional.of(overwriteCtx), false));
        Assertions.assertTrue(IcebergInsertExecutor.shouldExecuteEmptyInsert(
                true, Optional.of(staticOverwriteCtx), true));
    }
}
