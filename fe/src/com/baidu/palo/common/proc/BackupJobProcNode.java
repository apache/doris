// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.common.proc;

import com.baidu.palo.backup.AbstractBackupJob;
import com.baidu.palo.backup.BackupHandler;
import com.baidu.palo.common.AnalysisException;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

public class BackupJobProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TabletId").add("UnfinishedBackends")
            .build();

    private BackupHandler backupHandler;
    private long dbId;
    private Class<? extends AbstractBackupJob> jobClass;

    public BackupJobProcNode(BackupHandler backupHandler, long dbId, Class<? extends AbstractBackupJob> jobClass) {
        this.backupHandler = backupHandler;
        this.dbId = dbId;
        this.jobClass = jobClass;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        List<List<Comparable>> infos = backupHandler.getJobUnfinishedTablet(dbId, jobClass);
        for (List<Comparable> info : infos) {
            List<String> oneInfo = new ArrayList<String>(TITLE_NAMES.size());
            for (Comparable element : info) {
                oneInfo.add(element.toString());
            }
            result.addRow(oneInfo);
        }
        return result;
    }
}
