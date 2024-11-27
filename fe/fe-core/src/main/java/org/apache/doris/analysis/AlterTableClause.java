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

package org.apache.doris.analysis;

import org.apache.doris.alter.AlterOpType;

// alter table clause
public abstract class AlterTableClause extends AlterClause {

    public AlterTableClause(AlterOpType opType) {
        super(opType);
    }

    // if set to true, the corresponding table should be stable before processing this operation on it.
    protected boolean needTableStable = true;

    protected TableName tableName;

    public boolean isNeedTableStable() {
        return needTableStable;
    }

    public void setTableName(TableName tableName) {
        this.tableName = tableName;
    }

    public abstract boolean allowOpMTMV();

    public abstract boolean needChangeMTMVState();
}
