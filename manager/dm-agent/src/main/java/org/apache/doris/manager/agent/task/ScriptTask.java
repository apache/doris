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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ScriptTask extends Task<ScriptTaskDesc> {

    private static final Logger TASKLOG = LoggerFactory.getLogger(AgentConstants.LOG_TYPE_TASK);

    public ScriptTask(ScriptTaskDesc taskDesc) {
        super(taskDesc);
    }

    public ScriptTask(ScriptTaskDesc taskDesc, TaskHook taskHook) {
        super(taskDesc, taskHook);
    }

    @Override
    protected int execute() throws IOException, InterruptedException {
        Runtime rt = Runtime.getRuntime();
        String[] commands = {"/bin/bash", "-c", ""};
        commands[2] = taskDesc.getScriptCmd();
        Process proc = rt.exec(commands);
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

        TASKLOG.info("scriptCmd:{}", taskDesc.getScriptCmd());

        String s = null;
        while ((s = stdInput.readLine()) != null) {
            TASKLOG.info(s);
        }

        while ((s = stdError.readLine()) != null) {
            TASKLOG.info(s);
        }

        int exitVal = proc.waitFor();
        return exitVal;
    }
}
