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

import org.apache.doris.catalog.info.TableNameInfo;

/**
 * LockTableInfo is a wrapper class for LockTable to support TableNameInfo
 * which uses ConnectContext for analysis.
 */
public class LockTableInfo {

    /**
     * lock type.
     */
    public enum LockType {
        READ("READ"),
        READ_LOCAL("READ LOCAL"),
        WRITE("WRITE"),
        LOW_PRIORITY_WRITE("LOW_PRIORITY WRITE");
        private String desc;

        LockType(String description) {
            this.desc = description;
        }

        @Override
        public String toString() {
            return desc;
        }
    }

    private TableNameInfo tableNameInfo;
    private String alias;
    private LockType lockType;

    public LockTableInfo(TableNameInfo tableNameInfo, String alias, LockType lockType) {
        this.tableNameInfo = tableNameInfo;
        this.alias = alias;
        this.lockType = lockType;
    }

    public TableNameInfo getTableNameInfo() {
        return tableNameInfo;
    }

    public String getAlias() {
        return alias;
    }
}
