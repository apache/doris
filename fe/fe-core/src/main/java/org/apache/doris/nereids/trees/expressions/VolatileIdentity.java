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

package org.apache.doris.nereids.trees.expressions;

import com.google.common.base.Preconditions;

import java.util.Optional;

/** Value object for volatile per-call identity and temporary group-by binding comparison. */
public class VolatileIdentity {
    public static final VolatileIdentity NON_VOLATILE = new VolatileIdentity(Optional.empty(), false);

    private final Optional<ExprId> uniqueId;
    private final boolean ignoreUniqueId;

    public VolatileIdentity(ExprId uniqueId, boolean ignoreUniqueId) {
        this(Optional.of(uniqueId), ignoreUniqueId);
    }

    private VolatileIdentity(Optional<ExprId> uniqueId, boolean ignoreUniqueId) {
        Preconditions.checkArgument(!ignoreUniqueId || uniqueId.isPresent(),
                "ignoreUniqueId is meaningful only for volatile expressions");
        this.uniqueId = uniqueId;
        this.ignoreUniqueId = ignoreUniqueId;
    }

    public static VolatileIdentity newVolatileIdentity() {
        return new VolatileIdentity(StatementScopeIdGenerator.newExprId(), false);
    }

    public static VolatileIdentity of(ExprId uniqueId, boolean ignoreUniqueId) {
        return new VolatileIdentity(uniqueId, ignoreUniqueId);
    }

    public boolean isVolatile() {
        return uniqueId.isPresent();
    }

    public ExprId getUniqueId() {
        return uniqueId.get();
    }

    public Optional<ExprId> getUniqueIdOptional() {
        return uniqueId;
    }

    public boolean ignoreUniqueId() {
        return ignoreUniqueId;
    }

    public VolatileIdentity withIgnoreUniqueId(boolean ignoreUniqueId) {
        Preconditions.checkState(isVolatile(), "Only volatile expressions can ignore unique id");
        return new VolatileIdentity(uniqueId, ignoreUniqueId);
    }

    /** Compare volatile expressions by identity unless either side temporarily ignores it. */
    public boolean equalsByIdentity(VolatileIdentity other, boolean fallbackEquals) {
        if (!isVolatile() && !other.isVolatile()) {
            return fallbackEquals;
        }
        if (!isVolatile() || !other.isVolatile()) {
            return false;
        }
        if (ignoreUniqueId || other.ignoreUniqueId()) {
            return fallbackEquals;
        }
        return uniqueId.equals(other.getUniqueIdOptional());
    }

    public int hashCodeByIdentity(int fallbackHash) {
        if (!isVolatile() || ignoreUniqueId) {
            return fallbackHash;
        }
        return getUniqueId().asInt();
    }
}
