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

import org.apache.doris.thrift.TPushCooldownConfReq;
import org.apache.doris.thrift.TTaskType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PushCooldownConfTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(PushCooldownConfTask.class);

    private long cooldownReplicaId;
    private long cooldownTerm;

    public PushCooldownConfTask(long backendId, long dbId, long tableId, long partitionId, long indexId, long tabletId,
                                long cooldownReplicaId, long cooldownTerm) {
        super(null, backendId, TTaskType.PUSH_COOLDOWN_CONF, dbId, tableId, partitionId, indexId, tabletId);

        this.cooldownReplicaId = cooldownReplicaId;
        this.cooldownTerm = cooldownTerm;
    }

    public TPushCooldownConfReq toThrift() {
        TPushCooldownConfReq pushCooldownConfReq = new TPushCooldownConfReq();
        pushCooldownConfReq.setTabletId(tabletId);
        pushCooldownConfReq.setCooldownReplicaId(cooldownReplicaId);
        pushCooldownConfReq.setCooldownTerm(cooldownTerm);

        return pushCooldownConfReq;
    }
}
