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
package org.apache.doris.manager.agent.task;

import org.apache.doris.manager.agent.common.AgentConstants;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class TaskLruLog implements ITaskLog {
    private LRU<Integer, String> stdlogs;
    private LRU<Integer, String> errlogs;

    private int currentStdPage = 0;
    private int currentErrPage = 0;


    public TaskLruLog() {
        this(AgentConstants.TASK_LOG_ROW_MAX_COUNT, AgentConstants.TASK_LOG_ROW_MAX_COUNT);
    }

    public TaskLruLog(int maxStdCacheSize, int maxErrCacheSize) {
        this.stdlogs = new LRU<>(maxStdCacheSize);
        this.errlogs = new LRU<>(maxErrCacheSize);
    }

    @Override
    public Pair<Integer, List<String>> stdLog(int offset, int size) {
        int count = 0;
        ArrayList<String> list = new ArrayList<>();
        String temp = null;
        if (offset > stdlogs.size()) {
            return new Pair(stdlogs.size() - 1 > 0 ? stdlogs.size() - 1 : 0, list);
        }

        while (count < size && offset + count < stdlogs.size()) {
            if ((temp = stdlogs.get(offset + count)) != null) {
                list.add(temp);
            }
            count++;
        }
        Pair<Integer, List<String>> pair = new Pair(offset + count, list);
        return pair;
    }

    @Override
    public List<String> errLog(int offset, int size) {
        int count = 0;
        ArrayList<String> list = new ArrayList<>();
        String temp = null;
        while (count < size) {
            if ((temp = errlogs.get(offset + count)) != null) {
                list.add(temp);
            }
            count++;
        }
        return list;
    }

    public List<String> allStdLog() {
        return stdLog(0, stdlogs.size()).getValue();
    }

    public List<String> allErrLog() {
        return errLog(0, errlogs.size());
    }

    @Override
    public void appendStdLog(String log) {
        stdlogs.put(currentStdPage++, log);
    }

    @Override
    public void appendErrLog(String log) {
        errlogs.put(currentErrPage++, log);
    }
}
