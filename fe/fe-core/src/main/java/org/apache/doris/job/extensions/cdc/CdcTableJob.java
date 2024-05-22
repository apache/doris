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

package org.apache.doris.job.extensions.cdc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.CustomThreadFactory;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.InsertJob;
import org.apache.doris.nereids.util.DateUtils;
import org.apache.doris.nereids.util.StandardDateFormat;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.proto.InternalService;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CdcTableJob extends InsertJob {
    private static final Logger LOG = LogManager.getLogger(CdcTableJob.class);
    private static final long HEARTBEAT_TIME = 10000L;
    private static final int HEARTBEAT_MAX_RETRIES = 3;
    private static final long START_CDC_SERVER_TIMEOUT = 60000L;

    public static final ImmutableList<Column> SCHEMA = ImmutableList.of(
        new Column("Id", ScalarType.createStringType()),
        new Column("Name", ScalarType.createStringType()),
        new Column("Definer", ScalarType.createStringType()),
        new Column("ExecuteType", ScalarType.createStringType()),
        new Column("RecurringStrategy", ScalarType.createStringType()),
        new Column("Status", ScalarType.createStringType()),
        new Column("ExecuteSql", ScalarType.createStringType()),
        new Column("CreateTime", ScalarType.createStringType()),
        new Column("Comment", ScalarType.createStringType()),
        new Column("CdcServerProperties", ScalarType.createStringType()));

    public static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder<>();
        for (int i = 0; i < SCHEMA.size(); i++) {
            builder.put(SCHEMA.get(i).getName().toLowerCase(), i);
        }
        COLUMN_TO_INDEX = builder.build();
    }

    @SerializedName("cps")
    private Map<String, String> cdcProperties;
    @SerializedName("csps")
    private CdcServerProperties cdcServerProperties;
    private ScheduledExecutorService heartbeatService;
    private volatile boolean started = false;

    public CdcTableJob(String jobName,
                       JobStatus jobStatus,
                       String dbName,
                       String comment,
                       UserIdentity createUser,
                       JobExecutionConfiguration jobConfig,
                       Long createTimeMs,
                       String executeSql,
                       Map<String, String> cdcProperties) {
        super(jobName, jobStatus, dbName, comment, createUser, jobConfig, createTimeMs, executeSql);
        this.cdcProperties = cdcProperties;
        this.cdcServerProperties = new CdcServerProperties();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public void onRegister() throws JobException {
        super.onRegister();
        if(!checkCdcServerStarted()){
            if(cdcServerProperties.isEmpty()){
                createCdcServer();
            }
        }
    }

    @Override
    public void initialize() throws JobException {
        if(!cdcServerProperties.isEmpty()){
            scheduleHeartbeat(Pair.of(cdcServerProperties.getHost(), cdcServerProperties.getPort()));
        }else{
            Env.getCurrentEnv().getJobManager().getJob(getJobId()).updateJobStatus(JobStatus.PAUSED);
        }
    }

    private boolean checkCdcServerStarted() {
        Pair<String, Integer> ipPort = Pair.of(cdcServerProperties.getHost(), cdcServerProperties.getPort());
        try {
            if(RestService.isStarted(ipPort, getJobId())){
                LOG.info("cdc server already started.");
                return true;
            }
        } catch (IOException e) {
            LOG.warn("cdc server not running.");
        }
        return false;
    }

    private void createCdcServer() throws JobException {
        Backend backend = selectBackend();
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        String params = generateParams();
        //int cdcPort = startCdcJob(address, params);
        //cdcPort = 10000;

        int cdcPort = 10000;
        LOG.info("Cdc server started on backend: " + backend.getHost() + ":" + cdcPort);
        Pair<String, Integer> ipPort = Pair.of(backend.getHost(), cdcPort);
        waitCdcServerStarted(ipPort);
    }

    private void resumeCdcServer() throws JobException {
        //int cdcPort = startCdcJob(address, params);
        Pair<String, Integer> ipPort = Pair.of(cdcServerProperties.getHost(), cdcServerProperties.getPort());
        waitCdcServerStarted(ipPort);
    }

    private void waitCdcServerStarted(Pair<String, Integer> ipPort) throws JobException{
        long startTime = System.currentTimeMillis();
        while (true){
            try{
                if(RestService.isStarted(ipPort, getJobId())){
                    break;
                }
                if(System.currentTimeMillis() - startTime > START_CDC_SERVER_TIMEOUT){
                    throw new JobException("Cdc server start timeout");
                }
            }catch (IOException ex) {
                LOG.warn(ex.getMessage());
            }
            LOG.info("wait for cdc job to start...");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
        //set job state
        RestService.getSnapshotSplits(ipPort, getJobId());
        cdcServerProperties.setHost(ipPort.first);
        cdcServerProperties.setPort(ipPort.second);
    }

    private void scheduleHeartbeat(Pair<String, Integer> ipPort) {
        if(heartbeatService == null){
            heartbeatService = Executors.newScheduledThreadPool(1,
                new CustomThreadFactory("cdc-table-server-heartbeat-service"));
            heartbeatService.scheduleWithFixedDelay(() -> heartbeat(ipPort), HEARTBEAT_TIME, HEARTBEAT_TIME, TimeUnit.MILLISECONDS);
        }
    }

    private void heartbeat(Pair<String, Integer> ipPort) {
        int attempts = 0;
        while (attempts++ < HEARTBEAT_MAX_RETRIES) {
            if (!RestService.sendHeartbeat(ipPort, getJobId())) {
                LOG.error("send heartbeat fail.");
            } else {
                LOG.info("send heartbeat success.");
                cdcServerProperties.setLastHeartbeat(LocalDateTime.now(DateUtils.getTimeZone()).format(StandardDateFormat.DATE_TIME_FORMATTER));
                started = true;
                return;
            }
        }
        if(attempts >= HEARTBEAT_MAX_RETRIES){
            //Three heartbeats failed, and the jar process was judged to be down.
            AbstractJob job = Env.getCurrentEnv().getJobManager().getJob(getJobId());
            if(JobStatus.RUNNING.equals(job.getJobStatus())){
                job.setJobStatus(JobStatus.PAUSED);
                started = false;
            }
        }
    }

    private int startCdcJob(TNetworkAddress address, String params) throws JobException {
        InternalService.PCdcJobStartRequest request =
            InternalService.PCdcJobStartRequest.newBuilder().setParams(params).build();
        InternalService.PCdcJobStartResult result = null;
        try {
            Future<InternalService.PCdcJobStartResult> future =
                BackendServiceProxy.getInstance().startCdcJobAsync(address, request);
            result = future.get();
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                throw new JobException("Failed to start cdc server on backend: " + result.getStatus().getErrorMsgs(0));
            }
            return result.getPort();
        }catch (RpcException | ExecutionException | InterruptedException ex){
            throw new JobException(ex);
        }
    }

    private String generateParams() {
        Long jobId = getJobId();
        List<Frontend> frontends = Env.getCurrentEnv().getFrontends(null);
        int httpPort = Env.getCurrentEnv().getMasterHttpPort();
        List<String> fenodes = frontends.stream().filter(Frontend::isAlive).map(v -> v.getHost() + ":" + httpPort).collect(Collectors.toList());
        ParamsBuilder paramsBuilder = new ParamsBuilder(jobId.toString(), String.join(",", fenodes), cdcProperties);
        return paramsBuilder.buildParams();
    }

    private Backend selectBackend() throws JobException {
        Backend backend = null;
        BeSelectionPolicy policy = null;
        Set<Tag> userTags = new HashSet<>();
        if(ConnectContext.get() != null){
            String qualifiedUser = ConnectContext.get().getQualifiedUser();
            userTags  = Env.getCurrentEnv().getAuth().getResourceTags(qualifiedUser);
        }

        policy = new BeSelectionPolicy.Builder()
            .addTags(userTags)
            .setEnableRoundRobin(true)
            .needLoadAvailable().build();
        List<Long> backendIds;
        backendIds = Env.getCurrentSystemInfo().selectBackendIdsByPolicy(policy, 1);
        if (backendIds.isEmpty()) {
            throw new JobException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
        }
        backend = Env.getCurrentSystemInfo().getBackend(backendIds.get(0));
        if (backend == null) {
            throw new JobException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
        }
        return backend;
    }

    @Override
    public void onUnRegister() throws JobException {
        super.onUnRegister();
        if(heartbeatService != null){
            heartbeatService.shutdown();
        }
    }

    @Override
    public void onStatusChanged(JobStatus oldStatus, JobStatus newStatus) throws JobException {
        super.onStatusChanged(oldStatus, newStatus);
        if(JobStatus.RUNNING.equals(oldStatus) && JobStatus.PAUSED.equals(newStatus)){
            started = false;
            Pair<String, Integer> ipPort = Pair.of(cdcServerProperties.getHost(), cdcServerProperties.getPort());
            RestService.getSnapshotSplits(ipPort, getJobId());
            RestService.stopCdcProcess(ipPort, getJobId());
            heartbeatService.shutdown();
            return;
        }

        if(JobStatus.PAUSED.equals(oldStatus) && JobStatus.RUNNING.equals(newStatus)){
            resumeCdcServer();
            scheduleHeartbeat(Pair.of(cdcServerProperties.getHost(), cdcServerProperties.getPort()));
        }
    }

    @Override
    public JobType getJobType() {
        return JobType.CDC_TABLE;
    }


    @Override
    public TRow getCommonTvfInfo() {
        TRow trow = super.getCommonTvfInfo();
        trow.addToColumnValue(new TCell().setStringVal(cdcServerProperties.toString()));
        return trow;
    }

//    @Override
//    public List<InsertTask> createTasks(TaskType taskType, Map<Object, Object> taskContext) {
//        List<InsertTask> newTasks = new ArrayList<>();
//        InsertTask task = new CdcTableTask(getLabelName(), getCurrentDbName(), getExecuteSql(), getCreateUser());
//        getIdToTasks().put(task.getTaskId(), task);
//        newTasks.add(task);
//        recordTask(task.getTaskId());
//        initTasks(newTasks, taskType);
//        return newTasks;
//    }

    @Override
    public boolean isReadyForScheduling(Map<Object, Object> taskContext) {
        return super.isReadyForScheduling(taskContext) && started;
    }

    @Override
    public void onReplayCreate() throws JobException {
    }

    @Override
    public void onReplayEnd(AbstractJob<?, Map<Object, Object>> replayJob) throws JobException {
    }
}
