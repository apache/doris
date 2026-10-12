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

package org.apache.doris.planner;

import org.apache.doris.common.Config;

/**
 * Shared context for scan planning/runtime decisions.
 * <p>
 * Keep this object immutable so scan nodes can safely cache it and
 * we can evolve fields incrementally in future.
 */
public final class ScanContext {
    public static final ScanContext EMPTY = new ScanContext("", Config.max_be_exec_version);

    private final String clusterName;
    private final int beExecVersion;

    private ScanContext(String clusterName, int beExecVersion) {
        this.clusterName = clusterName == null ? "" : clusterName;
        this.beExecVersion = beExecVersion;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getClusterName() {
        return clusterName;
    }

    public int getBeExecVersion() {
        return beExecVersion;
    }

    public static final class Builder {
        private String clusterName = "";
        private int beExecVersion = Config.be_exec_version;

        public Builder clusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        public Builder beExecVersion(int beExecVersion) {
            this.beExecVersion = beExecVersion;
            return this;
        }

        public ScanContext build() {
            return new ScanContext(clusterName, beExecVersion);
        }
    }
}
