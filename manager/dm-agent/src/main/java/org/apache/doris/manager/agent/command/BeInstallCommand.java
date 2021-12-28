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

import org.apache.doris.manager.agent.common.AgentConstants;
import org.apache.doris.manager.agent.exception.AgentException;
import org.apache.doris.manager.agent.register.AgentContext;
import org.apache.doris.manager.agent.service.BeService;
import org.apache.doris.manager.agent.service.ServiceContext;
import org.apache.doris.manager.agent.task.ITaskHandlerFactory;
import org.apache.doris.manager.agent.task.QueuedTaskHandlerFactory;
import org.apache.doris.manager.agent.task.ScriptTask;
import org.apache.doris.manager.agent.task.ScriptTaskDesc;
import org.apache.doris.manager.agent.task.Task;
import org.apache.doris.manager.agent.task.TaskHandlerFactory;
import org.apache.doris.manager.agent.task.TaskHook;
import org.apache.doris.manager.common.domain.BeInstallCommandRequestBody;
import org.apache.doris.manager.common.domain.CommandType;

import java.util.Objects;

public class BeInstallCommand extends Command {
    private BeInstallCommandRequestBody requestBody;

    public BeInstallCommand(BeInstallCommandRequestBody requestBody) {
        this.requestBody = requestBody;
    }

    @Override
    public Task setupTask() {
        validCommand();

        if (requestBody.getInstallDir().endsWith("/")) {
            requestBody.setInstallDir(requestBody.getInstallDir().substring(0, requestBody.getInstallDir().length() - 1));
        }

        BeInstallTaskDesc taskDesc = new BeInstallTaskDesc();

        String scriptCmd = AgentConstants.BASH_BIN;
        scriptCmd += AgentContext.getAgentInstallDir() + "/bin/install_be.sh ";
        scriptCmd += " --installDir " + requestBody.getInstallDir();
        scriptCmd += " --url " + requestBody.getPackageUrl();
        taskDesc.setScriptCmd(scriptCmd);
        taskDesc.setInstallDir(requestBody.getInstallDir());
        taskDesc.setCreateBeStorageDir(requestBody.isMkBeStorageDir());
        return new ScriptTask(taskDesc, new BeInstallTaskHook());
    }

    @Override
    public ITaskHandlerFactory setupTaskHandlerFactory() {
        return TaskHandlerFactory.getTaskHandlerFactory(QueuedTaskHandlerFactory.class);
    }

    @Override
    public CommandType setupCommandType() {
        return CommandType.INSTALL_BE;
    }

    private void validCommand() {
        if (Objects.isNull(requestBody.getInstallDir()) || Objects.isNull(requestBody.getPackageUrl())) {
            throw new AgentException("required parameters are missing in body param");
        }

        if (!requestBody.getInstallDir().startsWith("/")) {
            throw new AgentException("the installation path must use an absolute path");
        }
    }

    private static class BeInstallTaskDesc extends ScriptTaskDesc {
        private String installDir;
        private boolean createBeStorageDir;

        public String getInstallDir() {
            return installDir;
        }

        public void setInstallDir(String installDir) {
            this.installDir = installDir;
        }

        public boolean isCreateBeStorageDir() {
            return createBeStorageDir;
        }

        public void setCreateBeStorageDir(boolean createBeStorageDir) {
            this.createBeStorageDir = createBeStorageDir;
        }
    }

    private static class BeInstallTaskHook extends TaskHook<BeInstallTaskDesc> {
        @Override
        public void onSuccess(BeInstallTaskDesc taskDesc) {
            BeService beService = new BeService(taskDesc.getInstallDir());
            ServiceContext.register(beService);
        }
    }
}
