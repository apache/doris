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

package org.apache.doris.task;

import org.apache.doris.cooldown.CooldownDelete;
import org.apache.doris.thrift.TCooldownDeleteFile;
import org.apache.doris.thrift.TCooldownDeleteFileReq;
import org.apache.doris.thrift.TTaskType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class SendCooldownDeleteTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(SendCooldownDeleteTask.class);

    private List<CooldownDelete> cooldownDeleteList;

    public SendCooldownDeleteTask(long backendId, List<CooldownDelete> cooldownDeleteList) {
        super(null, backendId, TTaskType.COOLDOWN_DELETE_FILE, -1, -1, -1, -1, -1);

        this.cooldownDeleteList = cooldownDeleteList;
    }

    public TCooldownDeleteFileReq toThrift() {
        TCooldownDeleteFileReq cooldownDeleteFileReq = new TCooldownDeleteFileReq();
        for (CooldownDelete cooldownDelete : cooldownDeleteList) {
            TCooldownDeleteFile cooldownDeleteFile = new TCooldownDeleteFile();
            cooldownDeleteFile.setTabletId(cooldownDelete.getTabletId());
            cooldownDeleteFile.setDeleteId(cooldownDelete.getDeleteId());
            cooldownDeleteFileReq.addToCooldownDeleteFiles(cooldownDeleteFile);
        }
        return cooldownDeleteFileReq;
    }
}
