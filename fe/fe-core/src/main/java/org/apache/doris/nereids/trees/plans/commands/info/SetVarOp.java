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

import org.apache.doris.analysis.SetType;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

/**
 * SetVarOp
 */
public abstract class SetVarOp {
    private SetType type;

    /** constructor*/
    public SetVarOp(SetType type) {
        this.type = type;
    }

    /**validate*/
    public void validate(ConnectContext ctx) throws UserException {}

    public void run(ConnectContext ctx) throws Exception {}

    public SetType getType() {
        return type;
    }

    public void setType(SetType type) {
        this.type = type;
    }

    public String toSql() {
        return "";
    }

    public boolean needAuditEncryption() {
        return false;
    }

    public void afterForwardToMaster(ConnectContext ctx) throws Exception {}
}
