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

package org.apache.doris.stack.agent;

import lombok.extern.slf4j.Slf4j;
import org.apache.doris.stack.dao.ProcessInstanceRepository;
import org.apache.doris.stack.entity.ProcessInstanceEntity;
import org.apache.doris.stack.service.ProcessTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * agent task status check
 **/
@Component
@Slf4j
public class AgentTaskStatusRunner implements ApplicationRunner {

    private static final long REFRESH_TIME = 10 * 1000L;
    private static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    @Autowired
    private ProcessTask processTask;

    @Autowired
    private ProcessInstanceRepository processInstanceRepository;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        this.scheduler.scheduleWithFixedDelay(() -> {
            try {
                refreshTaskStatus();
            } catch (Exception ex) {
                log.error("refresh agent task fail:", ex);
                ex.printStackTrace();
            }
        }, REFRESH_TIME, REFRESH_TIME, TimeUnit.MILLISECONDS);
    }

    private void refreshTaskStatus() {
        List<ProcessInstanceEntity> processEntities = processInstanceRepository.queryProcessList();
        for (ProcessInstanceEntity process : processEntities) {
            processTask.refreshAgentTaskStatus(process.getId());
        }
    }
}
