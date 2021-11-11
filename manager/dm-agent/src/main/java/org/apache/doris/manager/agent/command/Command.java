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

import org.apache.doris.manager.agent.task.ITaskHandlerFactory;
import org.apache.doris.manager.agent.task.Task;
import org.apache.doris.manager.common.domain.CommandType;

public abstract class Command {
    protected CommandType commandType;
    private Task task;
    private ITaskHandlerFactory handlerFactory;

    public Command setup() {
        task = setupTask();
        commandType = setupCommandType();
        task.getTaskDesc().setTaskName(commandType.name());
        handlerFactory = setupTaskHandlerFactory();
        return this;
    }

    public abstract Task setupTask();

    public abstract CommandType setupCommandType();

    public abstract ITaskHandlerFactory setupTaskHandlerFactory();

    public Task execute() {
        handlerFactory.createTaskHandler().handle(task);
        return task;
    }

    public ITaskHandlerFactory getHandlerFactory() {
        return handlerFactory;
    }
}
