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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.qe.ConnectContext;

import java.util.Objects;

/**
 * drop mtmv info
 */
public class DropMTMVInfo {
    private final TableNameInfo mvName;
    private final boolean ifExists;

    public DropMTMVInfo(TableNameInfo mvName, boolean ifExists) {
        this.mvName = Objects.requireNonNull(mvName, "require mvName object");
        this.ifExists = Objects.requireNonNull(ifExists, "require ifExists object");
    }

    /**
     * analyze drop info
     *
     * @param ctx ConnectContext
     */
    public void analyze(ConnectContext ctx) {
    }

    /**
     * getMvName
     *
     * @return TableNameInfo
     */
    public TableNameInfo getMvName() {
        return mvName;
    }
}
