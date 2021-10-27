package org.apache.doris.stack.service;

import org.apache.doris.manager.common.domain.RResult;
import org.apache.doris.stack.model.request.TaskInfoReq;
import org.apache.doris.stack.model.request.TaskLogReq;

public interface AgentTask {

    /**
     * fetch task info
     */
    RResult taskInfo(TaskInfoReq taskInfo);

    /**
     * fetch log
     */
    RResult taskStdlog(TaskLogReq taskInfo);

    RResult taskErrlog(TaskLogReq taskInfo);
}
