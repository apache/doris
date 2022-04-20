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

package org.apache.doris.load;

import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class MiniEtlJobInfo extends EtlJobInfo {
    // be reports etl task status to fe, and fe also actively get etl task status from be every 5 times
    private static final int GET_STATUS_INTERVAL_TIMES = 5;
    // load checker check etl job status times
    private int checkTimes;

    // etlTaskId -> etlTaskInfo
    private Map<Long, MiniEtlTaskInfo> idToEtlTask;

    public MiniEtlJobInfo() {
        super();
        checkTimes = 0;
        idToEtlTask = Maps.newHashMap();
    }

    public boolean needGetTaskStatus() {
        if (++checkTimes % GET_STATUS_INTERVAL_TIMES == 0) {
            return true;
        }
        return false;
    }

    public Map<Long, MiniEtlTaskInfo> getEtlTasks() {
        return idToEtlTask;
    }

    public MiniEtlTaskInfo getEtlTask(long taskId) {
        return idToEtlTask.get(taskId);
    }

    public void setEtlTasks(Map<Long, MiniEtlTaskInfo> idToEtlTask) {
        this.idToEtlTask = idToEtlTask;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(idToEtlTask.size());
        for (MiniEtlTaskInfo taskInfo : idToEtlTask.values()) {
            taskInfo.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int taskNum = in.readInt();
        for (int i = 0; i < taskNum; ++i) {
            MiniEtlTaskInfo taskInfo = new MiniEtlTaskInfo();
            taskInfo.readFields(in);
            idToEtlTask.put(taskInfo.getId(), taskInfo);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        return true;
    }
}
