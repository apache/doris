package org.apache.doris.stack.controller;

import io.swagger.annotations.Api;
import org.apache.doris.manager.common.domain.RResult;
import org.apache.doris.stack.model.request.TaskInfoReq;
import org.apache.doris.stack.model.request.TaskLogReq;
import org.apache.doris.stack.service.AgentTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * agent task controller
 **/
@Api(tags = "Agent Task API")
@RestController
@RequestMapping("/api/agent")
public class AgentTaskController {

    @Autowired
    private AgentTask agentTask;

    /**
     * request task detail
     */
    @RequestMapping(value = "/task", method = RequestMethod.POST)
    public RResult taskInfo(@RequestBody TaskInfoReq taskInfo) {
        return agentTask.taskInfo(taskInfo);
    }

    /**
     * request task stdout log
     */
    @RequestMapping(value = "/stdlog", method = RequestMethod.POST)
    public RResult taskStdlog(@RequestBody TaskLogReq taskInfo) {
        return agentTask.taskStdlog(taskInfo);
    }

    /**
     * request task error log
     */
    @RequestMapping(value = "/errlog", method = RequestMethod.POST)
    public RResult taskErrlog(@RequestBody TaskLogReq taskInfo) {
        return agentTask.taskErrlog(taskInfo);
    }

}
