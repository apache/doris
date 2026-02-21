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

import java.util.Map;

/**
 * Insert command context for MaxCompute tables.
 */
public class MCInsertCommandContext extends BaseExternalTableInsertCommandContext {

    private Map<String, String> staticPartitionSpec;
    private boolean overwrite;
    private String sessionId;
    private long blockIdStart;
    private long blockIdCount;
    private String writeSessionId;

    public MCInsertCommandContext() {
    }

    public Map<String, String> getStaticPartitionSpec() {
        return staticPartitionSpec;
    }

    public void setStaticPartitionSpec(Map<String, String> staticPartitionSpec) {
        this.staticPartitionSpec = staticPartitionSpec;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public long getBlockIdStart() {
        return blockIdStart;
    }

    public void setBlockIdStart(long blockIdStart) {
        this.blockIdStart = blockIdStart;
    }

    public long getBlockIdCount() {
        return blockIdCount;
    }

    public void setBlockIdCount(long blockIdCount) {
        this.blockIdCount = blockIdCount;
    }

    public String getWriteSessionId() {
        return writeSessionId;
    }

    public void setWriteSessionId(String writeSessionId) {
        this.writeSessionId = writeSessionId;
    }
}
