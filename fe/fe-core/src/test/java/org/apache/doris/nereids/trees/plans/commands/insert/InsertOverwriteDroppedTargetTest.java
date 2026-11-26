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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * What the swap of an overwrite does when its target has been dropped underneath it.
 *
 * <p>The swap takes the target table's write lock, and a target dropped by a concurrent DROP no longer
 * offers one. Skipping the swap there and returning lets {@code run} acknowledge the overwrite with a
 * success for a table that holds nothing it wrote, which is the one outcome a client must not be told;
 * {@link InsertOverwriteTableCommand} raises instead, so the caller's catch drops the temp partitions and
 * the statement fails. This pins that branch: the table a lock could not be taken on has to be named in
 * the failure.
 *
 * <p>Driven directly rather than through a statement because the window needs a DROP concurrent with the
 * swap, which a regression test cannot place without an injection point of its own; the condition the
 * branch reads -- {@code writeLockIfExist()} answering false -- is what the mock supplies, and the
 * publication it is handed records whether it was reached at all.
 */
public class InsertOverwriteDroppedTargetTest {

    @Test
    public void targetDroppedBeforeTheSwapFailsTheOverwrite() {
        InsertOverwriteTableCommand command = new InsertOverwriteTableCommand(
                Mockito.mock(LogicalPlan.class), Optional.empty(), Optional.empty(), Optional.empty());
        OlapTable droppedTarget = Mockito.mock(OlapTable.class);
        Mockito.when(droppedTarget.getName()).thenReturn("iot_dropped_target");
        Mockito.when(droppedTarget.writeLockIfExist()).thenReturn(false);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        Mockito.when(ctx.getQueryIdentifier()).thenReturn("stmt[1, query-id]");
        AtomicBoolean published = new AtomicBoolean(false);

        UserException thrown = Assertions.assertThrows(UserException.class,
                () -> Deencapsulation.invoke(command, "publishTheOverwrite", droppedTarget,
                        new OlapInsertCommandContext(false, true), ctx,
                        (InsertOverwriteTableCommand.OverwritePublication) () -> published.set(true)));
        Assertions.assertTrue(thrown.getMessage() != null
                        && thrown.getMessage().contains("iot_dropped_target")
                        && thrown.getMessage().contains("was dropped"),
                "the failure has to name the table whose swap could not be issued, but was: "
                        + thrown.getMessage());
        Assertions.assertFalse(published.get(),
                "nothing may be published into a table whose write lock could not be taken");
    }
}
