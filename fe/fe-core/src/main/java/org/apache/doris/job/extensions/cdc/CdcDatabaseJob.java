package org.apache.doris.job.extensions.cdc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.Config;
import org.apache.doris.common.CustomThreadFactory;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.base.TimerDefinition;
import org.apache.doris.job.common.IntervalUnit;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.cdc.state.AbstractSourceSplit;
import org.apache.doris.job.extensions.cdc.state.BinlogSplit;
import org.apache.doris.job.extensions.cdc.state.SnapshotSplit;
import org.apache.doris.job.extensions.cdc.utils.CdcLoadConstants;
import org.apache.doris.job.extensions.cdc.utils.RestService;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.proto.InternalService;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.doris.job.extensions.cdc.utils.CdcService.selectBackend;

public class CdcDatabaseJob extends AbstractJob<CdcDatabaseTask, Map<Object, Object>> {
    private static final Logger LOG = LogManager.getLogger(CdcDatabaseJob.class);
    public static final String BINLOG_SPLIT_ID = "binlog-split";
    public static final String SPLIT_ID = "splitId";
    public static final String FINISH_SPLITS = "finishSplits";
    public static final String ASSIGNED_SPLITS = "assignedSplits";
    private static final long SPLIT_TIMEOUT_MS = 600000;
    private static ObjectMapper objectMapper = new ObjectMapper();

    public static final ImmutableList<Column> SCHEMA = ImmutableList.of(
        new Column("Id", ScalarType.createStringType()),
        new Column("Name", ScalarType.createStringType()),
        new Column("Definer", ScalarType.createStringType()),
        new Column("ExecuteType", ScalarType.createStringType()),
        new Column("RecurringStrategy", ScalarType.createStringType()),
        new Column("Status", ScalarType.createStringType()),
        new Column("CreateTime", ScalarType.createStringType()),
        new Column("Progress", ScalarType.createStringType()));

    public static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder();
        for (int i = 0; i < SCHEMA.size(); i++) {
            builder.put(SCHEMA.get(i).getName().toLowerCase(), i);
        }
        COLUMN_TO_INDEX = builder.build();
    }
    @SerializedName("did")
    private long dbId;
    @SerializedName("cf")
    private Map<String, String> config;
    @SerializedName("rs")
    List<SnapshotSplit> remainingSplits = new ArrayList<>();
    @SerializedName("as")
    Map<String, SnapshotSplit> assignedSplits = new HashMap<>();
    @SerializedName("sfo")
    Map<String, Map<String, String>> splitFinishedOffsets = new HashMap<>();
    List<String> alreadyProcessedTables = new ArrayList<>();
    List<String> remainingTables = new ArrayList<>();
    String currentSplitTable = null;
    @SerializedName("ibsa")
    boolean isBinlogSplitAssigned = false;
    @SerializedName("co")
    Map<String, String> currentOffset;
    @SerializedName("isf")
    boolean isSplitFinished = false;
    @SerializedName("ht")
    private ConcurrentLinkedQueue<CdcDatabaseTask> historyTasks = new ConcurrentLinkedQueue<>();
    private volatile boolean started = false;
    private static final int port = 10000;
    ExecutorService executor;

    public CdcDatabaseJob(long dbId, String jobName, Map<String, String> config, JobExecutionConfiguration jobExecutionConfiguration) {
        super(getNextJobId(), jobName, JobStatus.RUNNING, jobName,
            "",  ConnectContext.get().getCurrentUserIdentity(), jobExecutionConfiguration);
        this.config = config;
        this.dbId = dbId;
    }

    @Override
    protected void checkJobParamsInternal() {
    }

    @Override
    public ShowResultSetMetaData getTaskMetaData() {
        return null;
    }

    @Override
    public JobType getJobType() {
        return JobType.CDC;
    }

    @Override
    public List<CdcDatabaseTask> queryTasks() {
        return Lists.newArrayList(historyTasks);
    }

    @Override
    public void initialize() throws JobException {
        super.initialize();
    }

    @Override
    public void onRegister() throws JobException {
        super.onRegister();
        Backend backend = createCdcProcess();
        getSplitAsync(backend);
        started = true;
    }

    private void getSplitAsync(Backend backend){
        if(executor == null){
            CustomThreadFactory threadFactory = new CustomThreadFactory("split-chunks");
            executor = Executors.newSingleThreadExecutor(threadFactory);
        }
        executor.submit(() -> fetchSplits(backend));
        LOG.info("fetch split async");
    }

    private void fetchSplits(Backend backend) {
        List<? extends AbstractSourceSplit> splits = new ArrayList<>();
        try{
            //call be rpc
            Pair<String, Integer> ipPort = Pair.of(backend.getHost(), port);
            splits = RestService.getSplits(ipPort, getJobId(), config);
        }catch (Exception ex){
            LOG.error("Fail to get split, ", ex);
        }

        if(splits.isEmpty()){
            try {
                Env.getCurrentEnv().getJobManager().getJob(getJobId()).updateJobStatus(JobStatus.PAUSED);
            } catch (JobException e) {
                LOG.error("Change job status error, jobId {}", getJobId(), e);
            }
            return;
        }

        LOG.debug("fetch splits {}", splits);
        for (AbstractSourceSplit split : splits) {
            if(Objects.equals(split.getSplitId(), BINLOG_SPLIT_ID)){
                BinlogSplit binlogSplit = (BinlogSplit) split;
                currentOffset = binlogSplit.getOffset();
                isBinlogSplitAssigned = true;
                break;
            }else{
                SnapshotSplit snapshotSplit = (SnapshotSplit) split;
                remainingSplits.add(snapshotSplit);
            }
        }
        logUpdateOperation();
        isSplitFinished = true;
    }

    @Override
    public boolean isReadyForScheduling(Map<Object, Object> taskContext) {
        if(!CollectionUtils.isEmpty(getRunningTasks())){
            return false;
        }
        if(!remainingSplits.isEmpty()
            || isBinlogSplitAssigned
            || (!assignedSplits.isEmpty() && assignedSplits.size() == splitFinishedOffsets.size())){
            return true;
        }
        LOG.info("job not ready scheduling");
        return false;
    }

    @Override
    public List<CdcDatabaseTask> createTasks(TaskType taskType, Map<Object, Object> taskContext) {
        try{
            Map<String, String> readOffset = new HashMap<>();
            //Call the BE interface and pass host, port, jobId
            //select backends
            Backend backend = selectBackend();
            if(!isBinlogSplitAssigned){
                if(!remainingSplits.isEmpty()){
                    SnapshotSplit snapshotSplit = remainingSplits.remove(0);
                    readOffset = new ObjectMapper().convertValue(snapshotSplit, new TypeReference<Map<String, String>>() {});
                    assignedSplits.put(snapshotSplit.getSplitId(), snapshotSplit);
                }else{
                    if(assignedSplits.size() == splitFinishedOffsets.size()){
                        readOffset.put(SPLIT_ID, BINLOG_SPLIT_ID);
                        readOffset.put(FINISH_SPLITS, objectMapper.writeValueAsString(splitFinishedOffsets));
                        readOffset.put(ASSIGNED_SPLITS,objectMapper.writeValueAsString(assignedSplits));
                        isBinlogSplitAssigned = true;
                    }else{
                        throw new RuntimeException("miss split");
                    }
                }
            }else{
                readOffset.put(SPLIT_ID, BINLOG_SPLIT_ID);
                //todo: When fully entering the binlog phase, there is no need to pass splits
                if(!splitFinishedOffsets.isEmpty() && !assignedSplits.isEmpty()){
                    readOffset.put(FINISH_SPLITS, objectMapper.writeValueAsString(splitFinishedOffsets));
                    readOffset.put(ASSIGNED_SPLITS, objectMapper.writeValueAsString(assignedSplits));
                }
                if(currentOffset != null){
                    readOffset.putAll(currentOffset);
                }
            }
            Preconditions.checkArgument(readOffset != null && !readOffset.isEmpty() , "read offset is empty");
            CdcDatabaseTask cdcDatabaseTask = new CdcDatabaseTask(dbId, backend, getJobId(), readOffset, config);
            ArrayList<CdcDatabaseTask> tasks = new ArrayList<>();
            tasks.add(cdcDatabaseTask);
            super.initTasks(tasks, taskType);
            LOG.info("finish create cdc task, task: {}", cdcDatabaseTask);
            return tasks;
        }catch (Exception ex){
            LOG.error("Create task failed,", ex);
            throw new RuntimeException("Create task failed");
        }
    }

    @Override
    public void onUnRegister() throws JobException {
        super.onUnRegister();
        Pair<String, Integer> ipPort = Pair.of(selectBackend().getHost(), port);
        //be rpc close，all backends clear
        RestService.closeResource(ipPort, getJobId());
        if(executor != null){
            executor.shutdown();
        }
    }

    private Backend createCdcProcess() throws JobException {
        Backend backend = selectBackend();
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        String params = generateParams();
        //int cdcPort = startCdcJob(address, params);
        //cdcPort = 10000;

        int cdcPort = 10000;
        LOG.info("Cdc server started on backend: " + backend.getHost() + ":" + cdcPort);
        return backend;
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
        ParamsBuilder paramsBuilder = new ParamsBuilder(jobId.toString(), String.join(",", fenodes), config);
        return paramsBuilder.buildParams();
    }

    @Override
    public void onStatusChanged(JobStatus oldStatus, JobStatus newStatus) throws JobException {
        super.onStatusChanged(oldStatus, newStatus);
        if(JobStatus.PAUSED.equals(oldStatus) && JobStatus.RUNNING.equals(newStatus)){
            if(!isSplitFinished){
                //todo: Continue the previous split
                fetchSplits(selectBackend());
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public TRow getTvfInfo() {
        TRow trow = new TRow();
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(super.getJobId())));
        trow.addToColumnValue(new TCell().setStringVal(super.getJobName()));
        trow.addToColumnValue(new TCell().setStringVal(super.getCreateUser().getQualifiedUser()));
        trow.addToColumnValue(new TCell().setStringVal(super.getJobConfig().getExecuteType().name()));
        trow.addToColumnValue(new TCell().setStringVal(super.getJobConfig().convertRecurringStrategyToString()));
        trow.addToColumnValue(new TCell().setStringVal(super.getJobStatus().name()));
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(super.getCreateTimeMs())));
        String progress = FeConstants.null_string;
        if(currentOffset != null ){
            progress = new Gson().toJson(currentOffset);
        }else if(remainingSplits.isEmpty() && !isBinlogSplitAssigned && !assignedSplits.isEmpty()){
            progress = "initial split";
        } else if (!remainingSplits.isEmpty()) {
            progress = "snapshot read";
        } else if (remainingSplits.isEmpty() && !assignedSplits.isEmpty()) {

        }
        trow.addToColumnValue(new TCell().setStringVal(progress));
        return trow;
    }

    public void updateOffset(Map<String, String> meta){
        String splitId = meta.get(SPLIT_ID);
        if(splitId == null){
            return;
        }
        if(!BINLOG_SPLIT_ID.equals(splitId)){
            splitFinishedOffsets.put(splitId, meta);
        }else{
            currentOffset = meta;
        }
    }

    public static JobExecutionConfiguration generateJobExecConfig(Map<String, String> properties) {
        JobExecutionConfiguration jobExecutionConfiguration = new JobExecutionConfiguration();
        jobExecutionConfiguration.setExecuteType(JobExecuteType.RECURRING);
        TimerDefinition timerDefinition = new TimerDefinition();
        String interval = properties.get(CdcLoadConstants.MAX_BATCH_INTERVAL);
        timerDefinition.setInterval(Long.parseLong(interval));
        timerDefinition.setIntervalUnit(IntervalUnit.SECOND);
        jobExecutionConfiguration.setTimerDefinition(timerDefinition);
        return jobExecutionConfiguration;
    }

    public void recordTasks(CdcDatabaseTask tasks) {
        if (Config.max_persistence_task_count < 1) {
            return;
        }
        historyTasks.add(tasks);

        while (historyTasks.size() > Config.max_persistence_task_count) {
            historyTasks.poll();
        }
        Env.getCurrentEnv().getEditLog().logUpdateJob(this);
    }
}
