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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class AgentTaskCleanupDaemon extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(AgentTaskCleanupDaemon.class);

    public static final Integer MAX_FAILURE_TIMES = 3;

    private final Map<Long, Integer> beInactiveCheckFailures = Maps.newHashMap();

    public AgentTaskCleanupDaemon() {
        super("agent-task-cleanup", Config.agent_task_health_check_intervals_ms);
    }

    @Override
    protected void runAfterCatalogReady() {
        LOG.info("Begin to clean up inactive agent tasks");
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        infoService.getAllClusterBackends(false)
                .forEach(backend -> {
                    long id = backend.getId();
                    if (backend.isAlive()) {
                        beInactiveCheckFailures.remove(id);
                    } else {
                        Integer failureTimes = beInactiveCheckFailures.compute(id, (beId, failures) -> {
                            int updated = (failures == null ? 1 : failures + 1);
                            if (updated >= MAX_FAILURE_TIMES) {
                                removeInactiveBeAgentTasks(beId);
                            }
                            return updated;
                        });
                        LOG.info("Check failure on be={}, times={}", failureTimes, failureTimes);
                    }
                });

        LOG.info("Finish to clean up inactive agent tasks");
    }

    private void removeInactiveBeAgentTasks(Long beId) {
        AgentTaskQueue.removeTask(beId, (agentTask -> {
            String errMsg = "BE down, this agent task is aborted";
            if (agentTask instanceof PushTask) {
                PushTask task = ((PushTask) agentTask);
                task.countDownLatchWithStatus(beId, agentTask.getTabletId(), new Status(TStatusCode.ABORTED, errMsg));
            }
            agentTask.setFinished(true);
            agentTask.setErrorCode(TStatusCode.ABORTED);
            agentTask.setErrorMsg(errMsg);
            if (LOG.isDebugEnabled()) {
                LOG.debug("BE down, remove agent task: {}", agentTask);
            }
        }));
    }
}
