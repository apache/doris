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
import org.apache.doris.manager.common.domain.ServiceRole;
import org.apache.doris.stack.agent.AgentRest;
import org.apache.doris.stack.bean.SpringApplicationContext;
import org.apache.doris.stack.constants.Constants;
import org.apache.doris.stack.exceptions.JdbcException;
import org.apache.doris.stack.model.BeJoin;
import org.apache.doris.stack.runner.TaskContext;
import org.apache.doris.stack.util.JdbcUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * join be to cluster
 **/
@Slf4j
public class JoinBeTask extends AbstractTask {


    private AgentRest agentRest;

    public JoinBeTask(TaskContext taskContext) {
        super(taskContext);
        this.agentRest = SpringApplicationContext.getBean(AgentRest.class);
    }

    @Override
    public void handle() {
        BeJoin requestParams = (BeJoin) taskContext.getRequestParams();
        Connection conn = null;
        try {
            conn = JdbcUtil.getConnection(requestParams.getFeHost(), requestParams.getFePort());
        } catch (SQLException e) {
            throw new JdbcException("Failed to get fe's jdbc connection");
        }
        Properties beConf = agentRest.roleConfig(requestParams.getBeHost(), requestParams.getAgentPort(), ServiceRole.BE.name());
        String port = beConf.getProperty(Constants.KEY_BE_HEARTBEAT_PORT);
        String addBeSqlFormat = "ALTER SYSTEM ADD BACKEND %s:%s";
        String beSql = String.format(addBeSqlFormat, requestParams.getBeHost(), port);
        boolean flag = JdbcUtil.execute(conn, beSql);
        if (!flag) {
            log.error("add be node fail:{}", beSql);
        }
    }
}
