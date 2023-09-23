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

import org.apache.doris.catalog.Database;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.load.ExportMgr;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

// TODO(lingbin): think if need a sub node to show unfinished instances
public class ExportProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("Label").add("State").add("Progress")
            .add("TaskInfo").add("Path")
            .add("CreateTime").add("StartTime").add("FinishTime")
            .add("Timeout").add("ErrorMsg").add("OutfileInfo")
            .build();

    // label and state column index of result
    public static final int LABEL_INDEX = 1;
    public static final int STATE_INDEX = 2;

    private static final int LIMIT = 2000;

    private ExportMgr exportMgr;
    private Database db;

    public ExportProcNode(ExportMgr exportMgr, Database db) {
        this.exportMgr = exportMgr;
        this.db = db;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(exportMgr);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        List<List<String>> jobInfos;
        if (db == null) {
            jobInfos = exportMgr.getExportJobInfos(LIMIT);
        } else {
            jobInfos = exportMgr.getExportJobInfosByIdOrState(
                db.getId(), 0, "", false, null, null, LIMIT);
        }
        result.setRows(jobInfos);
        return result;
    }

    public static int analyzeColumn(String columnName) throws AnalysisException {
        for (String title : TITLE_NAMES) {
            if (title.equalsIgnoreCase(columnName)) {
                return TITLE_NAMES.indexOf(title);
            }
        }

        throw new AnalysisException("Title name[" + columnName + "] does not exist");
    }
}
