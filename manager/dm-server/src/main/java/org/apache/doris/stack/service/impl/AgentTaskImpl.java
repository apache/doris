package org.apache.doris.stack.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.doris.manager.common.domain.RResult;
import org.apache.doris.stack.agent.AgentCache;
import org.apache.doris.stack.agent.AgentRest;
import org.apache.doris.stack.entity.AgentEntity;
import org.apache.doris.stack.exceptions.ServerException;
import org.apache.doris.stack.model.request.TaskInfoReq;
import org.apache.doris.stack.model.request.TaskLogReq;
import org.apache.doris.stack.service.AgentTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * agent task service
 **/
@Service
@Slf4j
public class AgentTaskImpl implements AgentTask {

    @Autowired
    private AgentRest agentRest;

    @Autowired
    private AgentCache agentCache;

    @Override
    public RResult taskInfo(TaskInfoReq taskInfo) {
        Map<String, Object> param = new HashMap<>();
        param.put("taskId", taskInfo.getTaskId());
        RResult result = agentRest.taskInfo(taskInfo.getHost(), agentPort(taskInfo.getHost()), param);
        return result;
    }

    @Override
    public RResult taskStdlog(TaskLogReq taskInfo) {
        Map<String, Object> param = new HashMap<>();
        param.put("taskId", taskInfo.getTaskId());
        param.put("offset", taskInfo.getOffset());
        RResult result = agentRest.taskStdLog(taskInfo.getHost(), agentPort(taskInfo.getHost()), param);
        return result;
    }

    @Override
    public RResult taskErrlog(TaskLogReq taskInfo) {
        Map<String, Object> param = new HashMap<>();
        param.put("taskId", taskInfo.getTaskId());
        param.put("offset", taskInfo.getOffset());
        RResult result = agentRest.taskErrLog(taskInfo.getHost(), agentPort(taskInfo.getHost()), param);
        return result;
    }

    private int agentPort(String host) {
        AgentEntity agent = agentCache.agentInfo(host);
        if (agent == null) {
            throw new ServerException("query agent port fail");
        }
        return agent.getPort();
    }
}
