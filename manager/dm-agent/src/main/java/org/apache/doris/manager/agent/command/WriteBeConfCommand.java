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

package org.apache.doris.manager.agent.command;

import org.apache.doris.manager.agent.exception.AgentException;
import org.apache.doris.manager.agent.service.BeService;
import org.apache.doris.manager.agent.service.ServiceContext;
import org.apache.doris.manager.agent.task.ITaskHandlerFactory;
import org.apache.doris.manager.agent.task.QueuedTaskHandlerFactory;
import org.apache.doris.manager.agent.task.Task;
import org.apache.doris.manager.agent.task.TaskDesc;
import org.apache.doris.manager.agent.task.TaskHandlerFactory;
import org.apache.doris.manager.agent.task.TaskHook;
import org.apache.doris.manager.common.domain.CommandType;
import org.apache.doris.manager.common.domain.ServiceRole;
import org.apache.doris.manager.common.domain.WriteBeConfCommandRequestBody;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Objects;

public class WriteBeConfCommand extends BeCommand {
    private WriteBeConfCommandRequestBody requestBody;

    public WriteBeConfCommand(WriteBeConfCommandRequestBody requestBody) {
        this.requestBody = requestBody;
    }

    @Override
    public Task setupTask() {
        if (Objects.isNull(requestBody.getContent())) {
            throw new AgentException("configuration content cannot be empty");
        }

        WriteBeConfTaskDesc desc = new WriteBeConfTaskDesc();
        desc.setCreateStorageDir(requestBody.isCreateStorageDir());
        WriteBeConfTaskHook hook = new WriteBeConfTaskHook();

        return new Task<TaskDesc>(desc, hook) {
            @Override
            protected int execute() throws IOException {
                try (FileOutputStream fos = new FileOutputStream(ServiceContext.getServiceMap().get(ServiceRole.BE).getConfigFilePath());
                     OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                     BufferedWriter bw = new BufferedWriter(osw)) {
                    bw.write(requestBody.getContent());
                    bw.flush();
                }
                return 0;
            }
        };
    }

    @Override
    public ITaskHandlerFactory setupTaskHandlerFactory() {
        return TaskHandlerFactory.getTaskHandlerFactory(QueuedTaskHandlerFactory.class);
    }

    @Override
    public CommandType setupCommandType() {
        return CommandType.WRITE_BE_CONF;
    }

    private static class WriteBeConfTaskDesc extends TaskDesc {
        private boolean createStorageDir;

        public boolean isCreateStorageDir() {
            return createStorageDir;
        }

        public void setCreateStorageDir(boolean createStorageDir) {
            this.createStorageDir = createStorageDir;
        }
    }

    private static class WriteBeConfTaskHook extends TaskHook<WriteBeConfTaskDesc> {
        @Override
        public void onSuccess(WriteBeConfTaskDesc taskDesc) {
            BeService service = (BeService) ServiceContext.getServiceMap().get(ServiceRole.BE);
            service.load();

            if (taskDesc.isCreateStorageDir()) {
                service.createStrorageDir();
            }
        }
    }
}
