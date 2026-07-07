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

package org.apache.doris.load;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.ShowLoadCommand;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditEvent.EventType;
import org.apache.doris.plugin.audit.StreamLoadAuditEvent;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TLoadJob;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStreamLoadRecord;
import org.apache.doris.thrift.TStreamLoadRecordResult;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class StreamLoadRecordMgr extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(StreamLoadRecordMgr.class);

    public class StreamLoadItem {
        private String label;
        private long dbId;
        private String finishTime;

        public StreamLoadItem(String label, long dbId, String finishTime) {
            this.label = label;
            this.dbId = dbId;
            this.finishTime = finishTime;
        }

        public String getLabel() {
            return label;
        }

        public long getDbId() {
            return dbId;
        }

        public String getFinishTime() {
            return finishTime;
        }

        public List<String> getStatistics() {
            List<String> row = Lists.newArrayList();
            row.add(label);
            row.add(String.valueOf(dbId));
            row.add(finishTime);
            return row;
        }
    }

    class StreamLoadComparator implements Comparator<StreamLoadItem> {
        public int compare(StreamLoadItem s1, StreamLoadItem s2) {
            return s1.getFinishTime().compareTo(s2.getFinishTime());
        }
    }

    Queue<StreamLoadItem> streamLoadRecordHeap = new PriorityQueue<>(new StreamLoadComparator());
    private Map<Long, Map<String, StreamLoadRecord>> dbIdToLabelToStreamLoadRecord = Maps.newConcurrentMap();
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();


    public StreamLoadRecordMgr(String name, long intervalMs) {
        super(name, intervalMs);
    }

    public void addStreamLoadRecord(long dbId, String label, StreamLoadRecord streamLoadRecord) {
        writeLock();
        while (isQueueFull()) {
            StreamLoadItem record = streamLoadRecordHeap.poll();
            if (record != null) {
                String deLabel = record.getLabel();
                long deDbId = record.getDbId();

                Map<String, StreamLoadRecord> labelToStreamLoadRecord = dbIdToLabelToStreamLoadRecord.get(deDbId);
                Iterator<Map.Entry<String, StreamLoadRecord>> iterRecord
                        = labelToStreamLoadRecord.entrySet().iterator();
                while (iterRecord.hasNext()) {
                    String labelInMap = iterRecord.next().getKey();
                    if (labelInMap.equals(deLabel)) {
                        iterRecord.remove();
                        break;
                    }
                }
            }
        }

        StreamLoadItem record = new StreamLoadItem(label, dbId, streamLoadRecord.getFinishTime());
        streamLoadRecordHeap.offer(record);

        if (!dbIdToLabelToStreamLoadRecord.containsKey(dbId)) {
            dbIdToLabelToStreamLoadRecord.put(dbId, new ConcurrentHashMap<>());
        }
        Map<String, StreamLoadRecord> labelToStreamLoadRecord = dbIdToLabelToStreamLoadRecord.get(dbId);
        if (!labelToStreamLoadRecord.containsKey(label)) {
            labelToStreamLoadRecord.put(label, streamLoadRecord);
        } else if (labelToStreamLoadRecord.get(label).getFinishTime().compareTo(streamLoadRecord.getFinishTime()) < 0) {
            labelToStreamLoadRecord.put(label, streamLoadRecord);
        }
        writeUnlock();
    }

    public List<StreamLoadItem> getStreamLoadRecords() {
        return new ArrayList<>(streamLoadRecordHeap);
    }

    /**
     * LEGACY read path for SHOW STREAM LOAD. Serves rows from the FE-local cache populated by the
     * periodic pull in {@link #runAfterCatalogReady()}, so results lag behind actual Stream Load
     * completion by up to fetch_stream_load_record_interval_second.
     *
     * <p>Recommended query direction is {@code SELECT ... FROM information_schema.loads}, which
     * reads BE RocksDB on demand and is not tied to this cache. This method is kept for
     * compatibility with existing SHOW STREAM LOAD users.
     */
    public List<List<Comparable>> getStreamLoadRecordByDb(
            long dbId, String label, boolean accurateMatch, ShowLoadCommand.StreamLoadState state) {
        LinkedList<List<Comparable>> streamLoadRecords = new LinkedList<List<Comparable>>();

        readLock();
        try {
            if (!dbIdToLabelToStreamLoadRecord.containsKey(dbId)) {
                return streamLoadRecords;
            }

            List<StreamLoadRecord> streamLoadRecordList = Lists.newArrayList();
            Map<String, StreamLoadRecord> labelToStreamLoadRecord = dbIdToLabelToStreamLoadRecord.get(dbId);
            if (Strings.isNullOrEmpty(label)) {
                streamLoadRecordList.addAll(labelToStreamLoadRecord.values().stream().collect(Collectors.toList()));
            } else {
                // check label value
                if (accurateMatch) {
                    if (!labelToStreamLoadRecord.containsKey(label)) {
                        return streamLoadRecords;
                    }
                    streamLoadRecordList.add(labelToStreamLoadRecord.get(label));
                } else {
                    // non-accurate match
                    for (Map.Entry<String, StreamLoadRecord> entry : labelToStreamLoadRecord.entrySet()) {
                        if (entry.getKey().contains(label)) {
                            streamLoadRecordList.add(entry.getValue());
                        }
                    }
                }
            }

            for (StreamLoadRecord streamLoadRecord : streamLoadRecordList) {
                try {
                    if (state != null && !String.valueOf(state).equalsIgnoreCase(streamLoadRecord.getStatus())) {
                        continue;
                    }
                    // check auth
                    if (!Env.getCurrentEnv().getAccessManager()
                            .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME,
                                    streamLoadRecord.getDb(), streamLoadRecord.getTable(),
                                    PrivPredicate.LOAD)) {
                        continue;
                    }
                    streamLoadRecords.add(streamLoadRecord.getStreamLoadInfo());
                } catch (Exception e) {
                    continue;
                }

            }
            return streamLoadRecords;
        } finally {
            readUnlock();
        }
    }

    /**
     * Map a StreamLoadRecord to a TLoadJob row for the unified information_schema.loads table.
     * Stream Load records are historical completion records, so PROGRESS is always "100%"
     * and JOB_ID / CREATE_TIME / ETL_* / TRANSACTION_ID / ERROR_TABLETS are empty strings.
     * Stream-Load-specific details that have no first-class loads column go into TASK_INFO
     * (Db/Table/ClientIp) and JOB_DETAILS (row/byte/timing counters) as JSON, so no extra
     * top-level column is added to the unified table.
     */
    static TLoadJob streamLoadRecordToLoadJob(StreamLoadRecord record) {
        TLoadJob tJob = new TLoadJob();
        tJob.setJobId("");
        tJob.setLabel(record.getLabel());
        tJob.setState(record.getStatus());
        tJob.setProgress("100%");
        tJob.setType("STREAM_LOAD");
        tJob.setEtlInfo("");

        JsonObject taskInfo = new JsonObject();
        taskInfo.addProperty("Db", record.getDb());
        taskInfo.addProperty("Table", record.getTable());
        taskInfo.addProperty("ClientIp", record.getClientIp());
        tJob.setTaskInfo(taskInfo.toString());

        tJob.setErrorMsg(record.getMessage());
        tJob.setCreateTime("");
        tJob.setEtlStartTime("");
        tJob.setEtlFinishTime("");
        tJob.setLoadStartTime(record.getStartTime());
        tJob.setLoadFinishTime(record.getFinishTime());
        tJob.setUrl(record.getUrl());

        JsonObject jobDetails = new JsonObject();
        jobDetails.addProperty("TotalRows", record.getTotalRows());
        jobDetails.addProperty("LoadedRows", record.getLoadedRows());
        jobDetails.addProperty("FilteredRows", record.getFilteredRows());
        jobDetails.addProperty("UnselectedRows", record.getUnselectedRows());
        jobDetails.addProperty("LoadBytes", record.getLoadBytes());
        jobDetails.addProperty("BeginTxnTimeMs", record.getBeginTxnTimeMs());
        jobDetails.addProperty("StreamLoadPutTimeMs", record.getStreamLoadPutTimeMs());
        tJob.setJobDetails(jobDetails.toString());

        tJob.setTransactionId("");
        tJob.setErrorTablets("");
        tJob.setUser(record.getUser());
        tJob.setComment(record.getComment());
        tJob.setFirstErrorMsg(record.getFirstErrorMsg());
        return tJob;
    }

    /**
     * Build a StreamLoadRecord from a BE-returned TStreamLoadRecord.
     * Shared by the legacy periodic pull (SHOW STREAM LOAD cache) and the on-demand
     * information_schema.loads read path so both interpret the BE record identically.
     */
    private static StreamLoadRecord buildStreamLoadRecord(TStreamLoadRecord item,
            String startTime, String finishTime) {
        return new StreamLoadRecord(item.getLabel(), item.getDb(), item.getTbl(), item.getUserIp(),
                item.getStatus(), item.getMessage(), item.getUrl(),
                String.valueOf(item.getTotalRows()),
                String.valueOf(item.getLoadedRows()),
                String.valueOf(item.getFilteredRows()),
                String.valueOf(item.getUnselectedRows()),
                String.valueOf(item.getLoadBytes()),
                item.isSetBeginTxnTimeMs() ? String.valueOf(item.getBeginTxnTimeMs()) : "",
                item.isSetStreamLoadPutTimeMs() ? String.valueOf(item.getStreamLoadPutTimeMs()) : "",
                startTime, finishTime, item.getUser(), item.getComment(),
                String.valueOf(item.getFirstErrorMsg()));
    }

    /**
     * Read Stream Load records directly from every alive BE's RocksDB stream-load-record store
     * and return them as TLoadJob rows for information_schema.loads.
     *
     * <p>This is the query path for {@code SELECT ... FROM information_schema.loads}. Unlike
     * SHOW STREAM LOAD, it does NOT depend on the FE periodic cache populated by
     * {@code fetch_stream_load_record_interval_second}; each query pulls fresh records on demand.
     *
     * <p>Robustness/bounds:
     * <ul>
     *   <li>A single unavailable BE is tolerated: its records are skipped instead of failing the
     *       whole system-table query.</li>
     *   <li>Each BE is paginated with the BE-side {@code stream_load_record_batch_size} bound, and
     *       the total number of rows returned is capped by {@code max_stream_load_record_size} to
     *       avoid exhausting FE memory on one SELECT.</li>
     * </ul>
     *
     * <p>The first version does no SQL predicate pushdown; filtering is handled by the schema-scan
     * layer above this call.
     */
    public List<TLoadJob> getStreamLoadJobsFromBackends() {
        List<TLoadJob> streamLoadJobs = Lists.newArrayList();
        ImmutableMap<Long, Backend> backends;
        try {
            backends = Env.getCurrentSystemInfo().getAllBackendsByAllCluster();
        } catch (AnalysisException e) {
            LOG.warn("Failed to load backends when reading stream load records for"
                    + " information_schema.loads", e);
            return streamLoadJobs;
        }

        int totalCap = Config.max_stream_load_record_size;
        for (Backend backend : backends.values()) {
            if (streamLoadJobs.size() >= totalCap) {
                break;
            }
            if (!backend.isAlive()) {
                continue;
            }
            readStreamLoadJobsFromBackend(backend, streamLoadJobs, totalCap);
        }
        return streamLoadJobs;
    }

    private void readStreamLoadJobsFromBackend(Backend backend, List<TLoadJob> streamLoadJobs, int totalCap) {
        // Paginate forward through the BE RocksDB records. Keys are "{finishTimeMillis}_{label}"
        // ordered by finish time; -1 means SeekToFirst on the BE side.
        long lastStreamLoadTime = -1;
        while (streamLoadJobs.size() < totalCap) {
            BackendService.Client client = null;
            TNetworkAddress address = null;
            boolean ok = false;
            Map<String, TStreamLoadRecord> batch;
            try {
                address = new TNetworkAddress(backend.getHost(), backend.getBePort());
                client = ClientPool.backendPool.borrowObject(address);
                TStreamLoadRecordResult result = client.getStreamLoadRecord(lastStreamLoadTime);
                batch = result.getStreamLoadRecord();
                ok = true;
            } catch (Exception e) {
                // Tolerate a single BE being temporarily unavailable: skip it and keep the
                // rest of information_schema.loads queryable.
                LOG.warn("Failed to read stream load records from backend[{}] for"
                        + " information_schema.loads, skipping this backend", backend.getId(), e);
                return;
            } finally {
                if (client != null) {
                    if (ok) {
                        ClientPool.backendPool.returnObject(address, client);
                    } else {
                        ClientPool.backendPool.invalidateObject(address, client);
                    }
                }
            }

            if (batch == null || batch.isEmpty()) {
                return;
            }

            long maxFinishTime = lastStreamLoadTime;
            for (TStreamLoadRecord item : batch.values()) {
                if (item.getFinishTime() > maxFinishTime) {
                    maxFinishTime = item.getFinishTime();
                }
                if (streamLoadJobs.size() >= totalCap) {
                    break;
                }
                try {
                    // Keep the same table-level LOAD privilege check call used by SHOW STREAM LOAD.
                    if (!Env.getCurrentEnv().getAccessManager()
                            .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME,
                                    item.getDb(), item.getTbl(), PrivPredicate.LOAD)) {
                        continue;
                    }
                    String startTime = TimeUtils.longToTimeString(item.getStartTime(),
                            TimeUtils.getDatetimeMsFormatWithTimeZone());
                    String finishTime = TimeUtils.longToTimeString(item.getFinishTime(),
                            TimeUtils.getDatetimeMsFormatWithTimeZone());
                    streamLoadJobs.add(streamLoadRecordToLoadJob(
                            buildStreamLoadRecord(item, startTime, finishTime)));
                } catch (Exception e) {
                    LOG.debug("Skip stream load record in information_schema.loads, label: {}",
                            item.getLabel(), e);
                }
            }

            // Stop when the pagination cursor can no longer advance. The next fetch seeks past
            // maxFinishTime, so a non-advancing cursor means there are no more pages (and this
            // guards against an infinite loop if a page is entirely at the boundary time).
            if (maxFinishTime <= lastStreamLoadTime) {
                return;
            }
            lastStreamLoadTime = maxFinishTime;
        }
    }

    public void clearStreamLoadRecord() {
        writeLock();
        if (streamLoadRecordHeap.size() > 0 || dbIdToLabelToStreamLoadRecord.size() > 0) {
            streamLoadRecordHeap.clear();
            dbIdToLabelToStreamLoadRecord.clear();
        }
        writeUnlock();
    }

    public boolean isQueueFull() {
        return streamLoadRecordHeap.size() >= Config.max_stream_load_record_size;
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    /**
     * LEGACY compatibility path. This daemon periodically pulls Stream Load records from every BE
     * into the FE-local cache (dbIdToLabelToStreamLoadRecord) so that SHOW STREAM LOAD can read
     * them. It is controlled by enable_stream_load_record and
     * fetch_stream_load_record_interval_second.
     *
     * <p>New query entry points should use {@code SELECT ... FROM information_schema.loads}, which
     * reads BE RocksDB on demand via {@link #getStreamLoadJobsFromBackends()} and does not depend
     * on this cache. This periodic pull is retained only to keep SHOW STREAM LOAD working; do not
     * build new features on top of it.
     */
    @Override
    protected void runAfterCatalogReady() {
        ImmutableMap<Long, Backend> backends;
        try {
            backends = Env.getCurrentSystemInfo().getAllBackendsByAllCluster();
        } catch (AnalysisException e) {
            LOG.warn("Failed to load backends from system info", e);
            return;
        }
        long start = System.currentTimeMillis();
        int pullRecordSize = 0;
        Map<Long, Long> beIdToLastStreamLoad = Maps.newHashMap();
        for (Backend backend : backends.values()) {
            if (!backend.isAlive()) {
                continue;
            }
            BackendService.Client client = null;
            TNetworkAddress address = null;
            boolean ok = false;
            try {
                address = new TNetworkAddress(backend.getHost(), backend.getBePort());
                client = ClientPool.backendPool.borrowObject(address);
                TStreamLoadRecordResult result = client.getStreamLoadRecord(backend.getLastStreamLoadTime());
                Map<String, TStreamLoadRecord> streamLoadRecordBatch = result.getStreamLoadRecord();
                pullRecordSize += streamLoadRecordBatch.size();
                long lastStreamLoadTime = -1;
                for (Map.Entry<String, TStreamLoadRecord> entry : streamLoadRecordBatch.entrySet()) {
                    TStreamLoadRecord streamLoadItem = entry.getValue();
                    String startTime = TimeUtils.longToTimeString(streamLoadItem.getStartTime(),
                            TimeUtils.getDatetimeMsFormatWithTimeZone());
                    String finishTime = TimeUtils.longToTimeString(streamLoadItem.getFinishTime(),
                            TimeUtils.getDatetimeMsFormatWithTimeZone());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("receive stream load record info from backend: {}."
                                        + " label: {}, db: {}, tbl: {}, user: {}, user_ip: {},"
                                        + " status: {}, message: {}, error_url: {},"
                                        + " total_rows: {}, loaded_rows: {}, filtered_rows: {}, unselected_rows: {},"
                                        + " load_bytes: {}, start_time: {}, finish_time: {}, first_error_msg: {}.",
                                backend.getHost(), streamLoadItem.getLabel(), streamLoadItem.getDb(),
                                streamLoadItem.getTbl(), streamLoadItem.getUser(), streamLoadItem.getUserIp(),
                                streamLoadItem.getStatus(), streamLoadItem.getMessage(), streamLoadItem.getUrl(),
                                streamLoadItem.getTotalRows(), streamLoadItem.getLoadedRows(),
                                streamLoadItem.getFilteredRows(), streamLoadItem.getUnselectedRows(),
                                streamLoadItem.getLoadBytes(), startTime, finishTime,
                                streamLoadItem.getFirstErrorMsg());
                    }

                    AuditEvent auditEvent =
                            new StreamLoadAuditEvent.AuditEventBuilder().setEventType(EventType.STREAM_LOAD_FINISH)
                                    .setLabel(streamLoadItem.getLabel()).setDb(streamLoadItem.getDb())
                                    .setTable(streamLoadItem.getTbl()).setUser(streamLoadItem.getUser())
                                    .setClientIp(streamLoadItem.getUserIp()).setStatus(streamLoadItem.getStatus())
                                    .setMessage(streamLoadItem.getMessage()).setUrl(streamLoadItem.getUrl())
                                    .setTotalRows(streamLoadItem.getTotalRows())
                                    .setLoadedRows(streamLoadItem.getLoadedRows())
                                    .setFilteredRows(streamLoadItem.getFilteredRows())
                                    .setUnselectedRows(streamLoadItem.getUnselectedRows())
                                    .setLoadBytes(streamLoadItem.getLoadBytes()).setStartTime(startTime)
                                    .setFinishTime(finishTime).build();
                    Env.getCurrentEnv().getAuditEventProcessor().handleAuditEvent(auditEvent);
                    if (entry.getValue().getFinishTime() > lastStreamLoadTime) {
                        lastStreamLoadTime = entry.getValue().getFinishTime();
                    }

                    if (Config.disable_show_stream_load) {
                        continue;
                    }
                    StreamLoadRecord streamLoadRecord =
                            buildStreamLoadRecord(streamLoadItem, startTime, finishTime);

                    String fullDbName = streamLoadItem.getDb();
                    Database db = Env.getCurrentInternalCatalog().getDbNullable(fullDbName);
                    if (db == null) {
                        String dbName = fullDbName;
                        if (Strings.isNullOrEmpty(streamLoadItem.getCluster())) {
                            dbName = streamLoadItem.getDb();
                        }
                        LOG.warn("unknown database, database=" + dbName);
                        continue;
                    }
                    long dbId = db.getId();
                    Env.getCurrentEnv().getStreamLoadRecordMgr()
                            .addStreamLoadRecord(dbId, streamLoadItem.getLabel(), streamLoadRecord);
                }

                if (streamLoadRecordBatch.size() > 0) {
                    backend.setLastStreamLoadTime(lastStreamLoadTime);
                    beIdToLastStreamLoad.put(backend.getId(), lastStreamLoadTime);
                } else {
                    beIdToLastStreamLoad.put(backend.getId(), backend.getLastStreamLoadTime());
                }

                ok = true;
            } catch (Exception e) {
                LOG.warn("task exec error. backend[{}]", backend.getId(), e);
            } finally {
                if (ok) {
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    ClientPool.backendPool.invalidateObject(address, client);
                }
            }
        }
        LOG.info("finished to pull stream load records of all backends. record size: {}, cost: {} ms",
                                                        pullRecordSize, (System.currentTimeMillis() - start));
        if (pullRecordSize > 0) {
            FetchStreamLoadRecord fetchStreamLoadRecord = new FetchStreamLoadRecord(beIdToLastStreamLoad);
            Env.getCurrentEnv().getEditLog().logFetchStreamLoadRecord(fetchStreamLoadRecord);
        }

        if (Config.disable_show_stream_load) {
            Env.getCurrentEnv().getStreamLoadRecordMgr().clearStreamLoadRecord();
        }
    }

    public void replayFetchStreamLoadRecord(FetchStreamLoadRecord fetchStreamLoadRecord) throws AnalysisException {
        ImmutableMap<Long, Backend> backends = Env.getCurrentSystemInfo().getAllBackendsByAllCluster();
        Map<Long, Long> beIdToLastStreamLoad = fetchStreamLoadRecord.getBeIdToLastStreamLoad();
        for (Backend backend : backends.values()) {
            if (beIdToLastStreamLoad.containsKey(backend.getId())) {
                long lastStreamLoadTime = beIdToLastStreamLoad.get(backend.getId());
                LOG.info("Replay stream load bdbje. backend: {}, last stream load time: {}",
                        backend.getHost(), lastStreamLoadTime);
                backend.setLastStreamLoadTime(lastStreamLoadTime);
            }
        }
    }

    public static class FetchStreamLoadRecord implements Writable {
        @SerializedName("beIdToLastStreamLoad")
        private Map<Long, Long> beIdToLastStreamLoad;

        public FetchStreamLoadRecord(Map<Long, Long> beIdToLastStreamLoad) {
            this.beIdToLastStreamLoad = beIdToLastStreamLoad;
        }

        public void setBeIdToLastStreamLoad(Map<Long, Long> beIdToLastStreamLoad) {
            this.beIdToLastStreamLoad = beIdToLastStreamLoad;
        }

        public Map<Long, Long> getBeIdToLastStreamLoad() {
            return beIdToLastStreamLoad;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            String json = GsonUtils.GSON.toJson(this);
            Text.writeString(out, json);
        }

        public static FetchStreamLoadRecord read(DataInput in) throws IOException {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, FetchStreamLoadRecord.class);
        }
    }
}
