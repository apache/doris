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

/**
 * The context of insert command.
 * You can add some fields or methods here if you need in derived classed
 */
public abstract class InsertCommandContext {

    /**
     * Set when the insert's transaction has committed: what it wrote -- its rows, and any table stream offset
     * update it carried -- is durable. That is not the same as the insert having succeeded. A load whose
     * publication times out after its commit is committed, while the session's visibility-timeout mode reports
     * it to the client as an error; and a load whose plan folded to an empty relation commits nothing at all,
     * since it runs no transaction unless it has an offset to commit.
     *
     * <p>Whoever owns a transaction records it where that commit happens; see
     * {@code AbstractInsertExecutor#markCommitted()}. A caller that owns what happens next -- an overwrite
     * publishing its temporary partitions -- decides by it: what is durable has to be published, and where
     * nothing is durable a failure or a cancellation still has everything to take back.
     */
    private boolean committed = false;

    public boolean hasCommitted() {
        return committed;
    }

    public void setCommitted(boolean committed) {
        this.committed = committed;
    }
}
