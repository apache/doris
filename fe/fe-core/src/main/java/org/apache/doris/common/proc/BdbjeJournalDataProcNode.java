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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.journal.bdbje.BDBDebugger;
import org.apache.doris.persist.OperationType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

// SHOW PROC "/bdbje/dbname/journalID"
public class BdbjeJournalDataProcNode implements ProcNodeInterface  {

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JournalId").add("OpType").add("Size").add("Data").build();

    private String dbName;
    private Long journalId;

    public BdbjeJournalDataProcNode(String dbName, Long journalId) {
        this.dbName = dbName;
        this.journalId = journalId;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        if (!Config.enable_bdbje_debug_mode) {
            throw new AnalysisException("Not in bdbje debug mode");
        }

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        BDBDebugger.BDBDebugEnv env = BDBDebugger.get().getEnv();
        BDBDebugger.JournalEntityWrapper entity = env.getJournalEntity(dbName, journalId);

        short opCode = entity.entity == null ? -1 : entity.entity.getOpCode();
        String data = entity.entity == null ? entity.errMsg : entity.entity.getData().toString();
        result.addRow(Lists.newArrayList(entity.journalId.toString(),
                    OperationType.getOpName(opCode), entity.size.toString(), data));

        return result;
    }
}
