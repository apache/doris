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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.transaction.GlobalTransactionMgrIface;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

public class TransStateProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("State").add("Number")
            .build();

    private long dbId;

    public TransStateProcDir(Long dbId) {
        this.dbId = dbId;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        GlobalTransactionMgrIface transactionMgr = Env.getCurrentGlobalTransactionMgr();
        result.setRows(transactionMgr.getDbTransStateInfo(dbId));
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String state) throws AnalysisException {
        if (Strings.isNullOrEmpty(state)) {
            throw new AnalysisException("State is not set");
        }

        if (!state.equals("running") && !state.equals("finished")) {
            throw new AnalysisException("State is invalid");
        }

        return new TransProcDir(dbId, state);
    }
}
