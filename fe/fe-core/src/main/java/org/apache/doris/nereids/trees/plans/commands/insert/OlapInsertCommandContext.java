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
 * For Olap Table
 */
public class OlapInsertCommandContext extends InsertCommandContext {
    private final boolean allowAutoPartition;
    private final boolean autoDetectOverwrite;
    private final long overwriteGroupId;
    private final boolean isOverwrite;

    public OlapInsertCommandContext(boolean allowAutoPartition, boolean autoDetectOverwrite, long overwriteGroupId,
            boolean isOverwrite) {
        this.allowAutoPartition = allowAutoPartition;
        this.autoDetectOverwrite = autoDetectOverwrite;
        this.overwriteGroupId = overwriteGroupId;
        this.isOverwrite = isOverwrite;
    }

    public OlapInsertCommandContext(boolean allowAutoPartition) {
        this(allowAutoPartition, false, 0, false);
    }

    public OlapInsertCommandContext(boolean allowAutoPartition, boolean isOverwrite) {
        this(allowAutoPartition, false, 0, isOverwrite);
    }

    public boolean isAllowAutoPartition() {
        return allowAutoPartition;
    }

    public boolean isAutoDetectOverwrite() {
        return autoDetectOverwrite;
    }

    public long getOverwriteGroupId() {
        return overwriteGroupId;
    }

    public boolean isOverwrite() {
        return isOverwrite;
    }
}
