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

package org.apache.doris.task;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.ThriftUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TAgentTaskRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TTaskType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/*
 * Like AgentBatchTask, but this class is used to submit tasks to BE in a bounded way, to avoid BE OOM.
 */
public class AgentBoundedBatchTask extends AgentBatchTask {
    private static final Logger LOG = LogManager.getLogger(AgentBoundedBatchTask.class);
    private static final int RPC_MAX_RETRY_TIMES = 3;

    private int taskConcurrency;
    private Map<Long, Integer> backendIdToConsumedTaskIndex;
    private int beUnavailableMaxLostTimeSecond;

    /**
     * NOTE:
     * this class is used to submit tasks to BE in a bounded way,
     * and it will automatically add to AgentTaskQueue.
     *
     * @param batchSize       the max number of tasks to submit to BE in one time
     * @param taskConcurrency the max number of tasks to submit to BE in one time
     */
    public AgentBoundedBatchTask(int batchSize, int taskConcurrency) {
        super(batchSize);
        this.taskConcurrency = taskConcurrency;
        this.backendIdToConsumedTaskIndex = new HashMap<>();
        this.beUnavailableMaxLostTimeSecond = Config.agent_task_be_unavailable_heartbeat_timeout_second;
    }

    @Override
    public void addTask(AgentTask agentTask) {
        if (agentTask == null) {
            return;
        }
        long backendId = agentTask.getBackendId();
        if (backendIdToTasks.containsKey(backendId)) {
            List<AgentTask> tasks = backendIdToTasks.get(backendId);
            tasks.add(agentTask);
        } else {
            List<AgentTask> tasks = new ArrayList<>();
            tasks.add(agentTask);
            backendIdToTasks.put(backendId, tasks);
        }
    }

    @Override
    public void run() {
        int taskNum = getTaskNum();
        LOG.info("begin to submit tasks to BE. total {} tasks, be task concurrency: {}", taskNum, taskConcurrency);
        boolean submitFinished = false;
        while (getSubmitTaskNum() < taskNum && !submitFinished) {
            for (Long backendId : backendIdToTasks.keySet()) {
                int consumedTaskIndex = backendIdToConsumedTaskIndex.getOrDefault(backendId, 0);
                if (consumedTaskIndex >= backendIdToTasks.get(backendId).size()) {
                    LOG.info("backend {} has submitted all tasks, taskNum: {}",
                            backendId, backendIdToTasks.get(backendId).size());
                    continue;
                }

                boolean ok = false;
                String errMsg = "";
                Backend backend = null;
                List<AgentTask> tasks = new ArrayList<>();
                List<TAgentTaskRequest> agentTaskRequests = new ArrayList<>();
                try {
                    backend = Env.getCurrentSystemInfo().getBackend(backendId);
                    tasks = this.backendIdToTasks.getOrDefault(backendId, new ArrayList<>());
                    if (backend == null) {
                        errMsg = String.format("backend %d is not found", backendId);
                        throw new RuntimeException(errMsg);
                    }
                    if (!backend.isAlive()) {
                        errMsg = String.format("backend %d is not alive", backendId);
                        if (System.currentTimeMillis() - backend.getLastUpdateMs()
                                > beUnavailableMaxLostTimeSecond * 1000) {
                            errMsg = String.format("backend %d is not alive too long, last update time: %s",
                                    backendId, TimeUtils.longToTimeString(backend.getLastUpdateMs()));
                            throw new RuntimeException(errMsg);
                        }
                        continue;
                    }

                    int runningTaskNum = getRunningTaskNum(backendId);
                    LOG.info("backend {} has {} running tasks, task concurrency: {}",
                            backendId, runningTaskNum, taskConcurrency);
                    int index = consumedTaskIndex;
                    for (; index < tasks.size()
                            && index < consumedTaskIndex + taskConcurrency - runningTaskNum; index++) {
                        agentTaskRequests.add(toAgentTaskRequest(tasks.get(index)));
                        // add to AgentTaskQueue
                        AgentTaskQueue.addTask(tasks.get(index));
                        if (agentTaskRequests.size() >= batchSize) {
                            submitTasks(backend, agentTaskRequests);
                            agentTaskRequests.clear();
                        }
                    }
                    submitTasks(backend, agentTaskRequests);
                    backendIdToConsumedTaskIndex.put(backendId, index);
                    LOG.info("submit task to backend {} finished, already submitted task num: {}/{}",
                            backendId, index, tasks.size());
                    ok = true;
                } catch (Exception e) {
                    LOG.warn("task exec error. backend[{}]", backendId, e);
                    errMsg = String.format("task exec error: %s. backend[%d]", e.getMessage(), backendId);
                    if (!agentTaskRequests.isEmpty() && errMsg.contains("Broken pipe")) {
                        // Log the task binary message size and the max task type, to help debug the
                        // large thrift message size issue.
                        List<Pair<TTaskType, Long>> taskTypeAndSize = agentTaskRequests.stream()
                                .map(req -> Pair.of(req.getTaskType(), ThriftUtils.getBinaryMessageSize(req)))
                                .collect(Collectors.toList());
                        Pair<TTaskType, Long> maxTaskTypeAndSize = taskTypeAndSize.stream()
                                .max((p1, p2) -> Long.compare(p1.value(), p2.value()))
                                .orElse(null);  // taskTypeAndSize is not empty
                        TTaskType maxType = maxTaskTypeAndSize.first;
                        long maxSize = maxTaskTypeAndSize.second;
                        long totalSize = taskTypeAndSize.stream().map(Pair::value).reduce(0L, Long::sum);
                        LOG.warn("submit {} tasks to backend[{}], total size: {}, max task type: {}, size: {}. msg: {}",
                                agentTaskRequests.size(), backendId, totalSize, maxType, maxSize, e.getMessage());
                    }
                } finally {
                    if (!ok) {
                        submitFinished = true;
                        LOG.warn("submit task to backend {} failed, errMsg: {}, cancel all tasks", backendId, errMsg);
                        cancelAllTasks(errMsg);
                    }
                }
            }

            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                String errMsg = "submit task thread is interrupted";
                LOG.warn(errMsg, e);
                submitFinished = true;
                cancelAllTasks(errMsg);
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private static void submitTasks(Backend backend, List<TAgentTaskRequest> agentTaskRequests) throws Exception {
        long start = System.currentTimeMillis();
        if (agentTaskRequests.isEmpty()) {
            return;
        }

        if (LOG.isDebugEnabled()) {
            long size = agentTaskRequests.stream()
                    .map(ThriftUtils::getBinaryMessageSize)
                    .reduce(0L, Long::sum);
            TTaskType firstTaskType = agentTaskRequests.get(0).getTaskType();
            LOG.debug("submit {} tasks to backend[{}], total size: {}, first task type: {}",
                    agentTaskRequests.size(), backend.getId(), size, firstTaskType);
            for (TAgentTaskRequest req : agentTaskRequests) {
                LOG.debug("send task: type[{}], backend[{}], signature[{}]",
                        req.getTaskType(), backend.getId(), req.getSignature());
            }
        }

        MetricRepo.COUNTER_AGENT_TASK_REQUEST_TOTAL.increase(1L);

        BackendService.Client client = null;
        TNetworkAddress address = null;
        // create AgentClient
        String host = FeConstants.runningUnitTest ? "127.0.0.1" : backend.getHost();
        address = new TNetworkAddress(host, backend.getBePort());
        long backendId = backend.getId();
        boolean ok = false;
        for (int attempt = 1; attempt <= RPC_MAX_RETRY_TIMES; attempt++) {
            try {
                if (client == null) {
                    // borrow new client when previous client request failed
                    client = ClientPool.backendPool.borrowObject(address);
                }
                client.submitTasks(agentTaskRequests);
                ok = true;
                break;
            } catch (Exception e) {
                if (attempt == RPC_MAX_RETRY_TIMES) {
                    LOG.warn("submit task to agent failed. backend[{}], request size: {}, elapsed:{} ms error: {}",
                            backendId, agentTaskRequests.size(), System.currentTimeMillis() - start,
                            e.getMessage());
                    throw e;
                } else {
                    LOG.warn("submit task attempt {} failed, retrying... backend[{}], error: {}",
                            attempt, backendId, e.getMessage());
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            } finally {
                if (ok) {
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    ClientPool.backendPool.invalidateObject(address, client);
                    client = null;
                }
            }
        }
    }

    private int getSubmitTaskNum() {
        return backendIdToConsumedTaskIndex.values().stream()
                .mapToInt(Integer::intValue)
                .sum();
    }

    private int getFinishedTaskNum(long backendId) {
        int count = 0;
        for (AgentTask agentTask : this.backendIdToTasks.get(backendId)) {
            if (agentTask.isFinished()) {
                count++;
            }
        }
        return count;
    }

    private int getRunningTaskNum(long backendId) {
        int count = 0;
        List<AgentTask> tasks = backendIdToTasks.get(backendId);
        int consumedTaskIndex = backendIdToConsumedTaskIndex.getOrDefault(backendId, 0);
        for (int i = 0; i < consumedTaskIndex; i++) {
            if (!tasks.get(i).isFinished) {
                count++;
            }
        }
        return count;
    }

    private void cancelAllTasks(String errMsg) {
        for (List<AgentTask> beTasks : backendIdToTasks.values()) {
            for (AgentTask task : beTasks) {
                task.failedWithMsg(errMsg);
            }
        }
    }
}
