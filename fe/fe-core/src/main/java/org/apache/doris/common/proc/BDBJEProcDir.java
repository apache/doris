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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

// SHOW PROC "/bdbje"
public class BDBJEProcDir implements ProcDirInterface  {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("DbNames").add("JournalNumber").add("Comment").build();

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String dbName) throws AnalysisException {
        return new BDBJEDatabaseProcDir(dbName);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        if (!Config.enable_bdbje_debug_mode) {
            throw new AnalysisException("Not in bdbje debug mode");
        }

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        BDBDebugger.BDBDebugEnv debugEnv = BDBDebugger.get().getEnv();
        List<String> dbNames = debugEnv.listDbNames();
        TreeMap<String, Long> journalNumMap = new TreeMap<>();
        for (String dbName : dbNames) {
            journalNumMap.put(dbName, debugEnv.getJournalNumber(dbName));
        }

        for (Map.Entry<String, Long> entry : journalNumMap.entrySet()) {
            result.addRow(Lists.newArrayList(entry.getKey(), entry.getValue().toString(), ""));
        }
        return result;
    }
}
