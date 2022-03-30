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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.load.Load;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class LoadProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("Label").add("State").add("Progress")
            .add("Type").add("EtlInfo").add("TaskInfo").add("ErrorMsg").add("CreateTime")
            .add("EtlStartTime").add("EtlFinishTime").add("LoadStartTime").add("LoadFinishTime")
            .add("URL").add("JobDetails").add("TransactionId").add("ErrorTablets")
            .build();

    // label and state column index of result
    public static final int LABEL_INDEX = 1;
    public static final int STATE_INDEX = 2;
    public static final int ERR_MSG_INDEX = 7;
    public static final int URL_INDEX = 13;
    public static final int JOB_DETAILS_INDEX = 14;

    private static final int LIMIT = 2000;

    private Load load;
    private Database db;

    public LoadProcDir(Load load, Database db) {
        this.load = load;
        this.db = db;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(load);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        // merge load job from load and loadManager
        LinkedList<List<Comparable>> loadJobInfos = load.getLoadJobInfosByDb(db.getId(), db.getFullName(),
                null, false, null);
        loadJobInfos.addAll(Catalog.getCurrentCatalog().getLoadManager().getLoadJobInfosByDb(db.getId(), null,
                false,
                null));
        int counter = 0;
        Iterator<List<Comparable>> iterator = loadJobInfos.descendingIterator();
        while (iterator.hasNext()) {
            List<Comparable> infoStr = iterator.next();
            List<String> oneInfo = new ArrayList<String>(TITLE_NAMES.size());
            for (Comparable element : infoStr) {
                oneInfo.add(element.toString());
            }
            result.addRow(oneInfo);
            if (++counter >= LIMIT) {
                break;
            }
        }
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String jobIdStr) throws AnalysisException {
        long jobId = -1L;
        try {
            jobId = Long.valueOf(jobIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid job id format: " + jobIdStr);
        }

        return new LoadJobProcNode(load, jobId);
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
