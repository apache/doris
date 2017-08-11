// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.qe;

import com.baidu.palo.analysis.SetNamesVar;
import com.baidu.palo.analysis.SetPassVar;
import com.baidu.palo.analysis.SetStmt;
import com.baidu.palo.analysis.SetTransaction;
import com.baidu.palo.analysis.SetVar;
import com.baidu.palo.common.DdlException;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

// Set executor
public class SetExecutor {
    private static final Logger LOG = LogManager.getLogger(SetExecutor.class);

    private ConnectContext ctx;
    private SetStmt stmt;

    public SetExecutor(ConnectContext ctx, SetStmt stmt) {
        this.ctx = ctx;
        this.stmt = stmt;
    }

    private void setVariable(SetVar var) throws DdlException {
        if (var instanceof SetPassVar) {
            // Set password
            SetPassVar setPassVar = (SetPassVar) var;
            ctx.getCatalog().getUserMgr().setPasswd(setPassVar.getUser(), setPassVar.getPassword());
        } else if (var instanceof SetNamesVar) {
            // do nothing
            return;
        } else if (var instanceof SetTransaction) {
            // do nothing
            return;
        } else {
            VariableMgr.setVar(ctx.getSessionVariable(), var);
        }
    }

    public void execute() throws DdlException {
        for (SetVar var : stmt.getSetVars()) {
            setVariable(var);
        }
    }
}
