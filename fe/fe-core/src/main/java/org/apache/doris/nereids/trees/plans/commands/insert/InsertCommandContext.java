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
     * Set when the insert that ran under this context finished on the path that begins no transaction at
     * all: its plan folded to an empty relation and there was no table stream offset to commit. Nothing
     * such an insert did is durable -- no row, no offset -- which is what a caller that owns what happens
     * next has to know before it decides whether a cancellation arriving from there on still has anything
     * to take back. See {@link InsertIntoTableCommand#runInternal} for the one path that sets it and
     * {@code InsertOverwriteTableCommand#run} for the caller that reads it.
     */
    private boolean committedNothing = false;

    public boolean hasCommittedNothing() {
        return committedNothing;
    }

    public void setCommittedNothing(boolean committedNothing) {
        this.committedNothing = committedNothing;
    }
}
