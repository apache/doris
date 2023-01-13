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

import org.apache.doris.cooldown.CooldownConf;
import org.apache.doris.thrift.TCooldownConf;
import org.apache.doris.thrift.TPushCooldownConfReq;
import org.apache.doris.thrift.TTaskType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class PushCooldownConfTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(PushCooldownConfTask.class);

    private List<CooldownConf> cooldownConfList;

    public PushCooldownConfTask(long backendId, List<CooldownConf> cooldownConfList) {
        super(null, backendId, TTaskType.PUSH_COOLDOWN_CONF, -1, -1, -1, -1, -1);

        this.cooldownConfList = cooldownConfList;
    }

    public TPushCooldownConfReq toThrift() {
        TPushCooldownConfReq pushCooldownConfReq = new TPushCooldownConfReq();
        for (CooldownConf cooldownConf : cooldownConfList) {
            TCooldownConf tCooldownConf = new TCooldownConf();
            tCooldownConf.setTabletId(cooldownConf.getTabletId());
            tCooldownConf.setCooldownReplicaId(cooldownConf.getCooldownReplicaId());
            tCooldownConf.setCooldownTerm(cooldownConf.getCooldownTerm());
            pushCooldownConfReq.addToCooldownConfs(tCooldownConf);
        }
        return pushCooldownConfReq;
    }
}
