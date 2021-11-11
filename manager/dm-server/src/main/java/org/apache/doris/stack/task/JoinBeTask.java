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

package org.apache.doris.stack.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.manager.common.domain.ServiceRole;
import org.apache.doris.stack.agent.AgentRest;
import org.apache.doris.stack.bean.SpringApplicationContext;
import org.apache.doris.stack.constants.Constants;
import org.apache.doris.stack.exceptions.JdbcException;
import org.apache.doris.stack.exceptions.ServerException;
import org.apache.doris.stack.model.task.BeJoin;
import org.apache.doris.stack.runner.TaskContext;
import org.apache.doris.stack.util.JdbcUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.Properties;

/**
 * join be to cluster
 **/
@Slf4j
public class JoinBeTask extends AbstractTask {

    private final String BE_EXIST_MSG = "Same backend already exists";
    private AgentRest agentRest;

    public JoinBeTask(TaskContext taskContext) {
        super(taskContext);
        this.agentRest = SpringApplicationContext.getBean(AgentRest.class);
    }

    @Override
    public Object handle() {
        BeJoin taskDesc = (BeJoin) taskContext.getTaskDesc();
        String backendHost = taskDesc.getBeHost();
        Properties beConf = agentRest.roleConfig(backendHost, taskDesc.getAgentPort(), ServiceRole.BE.name());
        String beHeatPort = beConf.getProperty(Constants.KEY_BE_HEARTBEAT_PORT);
        Connection conn = null;
        try {
            conn = JdbcUtil.getConnection(taskDesc.getFeHost(), taskDesc.getFeQueryPort());
        } catch (SQLException e) {
            log.error("get connection fail:", e);
            throw new JdbcException("Failed to get fe's jdbc connection");
        }

        String addBeSqlFormat = "ALTER SYSTEM ADD BACKEND \"%s:%s\"";
        String addBe = String.format(addBeSqlFormat, taskDesc.getBeHost(), beHeatPort);
        try {
            JdbcUtil.execute(conn, addBe);
            return "Add backend success";
        } catch (SQLException e) {
            if (e instanceof SQLSyntaxErrorException
                    && StringUtils.isNotBlank(e.getMessage())
                    && e.getMessage().contains(BE_EXIST_MSG)) {
                log.info("backend already add,response:{}", e.getMessage());
                return "Backend already exist";
            }
            log.error("Failed to add backend:{}:{}", taskDesc.getBeHost(), beHeatPort, e);
            throw new ServerException(e.getMessage());
        }
    }
}
