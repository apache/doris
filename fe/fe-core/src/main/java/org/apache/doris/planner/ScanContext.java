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

/**
 * Shared context for scan planning/runtime decisions.
 * <p>
 * Keep this object immutable so scan nodes can safely cache it and
 * we can evolve fields incrementally in future.
 */
public final class ScanContext {
    public static final ScanContext EMPTY = new ScanContext("");

    private final String clusterName;

    private ScanContext(String clusterName) {
        this.clusterName = clusterName == null ? "" : clusterName;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getClusterName() {
        return clusterName;
    }

    public static final class Builder {
        private String clusterName = "";

        public Builder clusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        public ScanContext build() {
            if (clusterName == null || clusterName.isEmpty()) {
                return ScanContext.EMPTY;
            }
            return new ScanContext(clusterName);
        }
    }
}
