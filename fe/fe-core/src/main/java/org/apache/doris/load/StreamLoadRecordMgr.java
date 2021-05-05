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

import org.apache.doris.analysis.ShowStreamLoadStmt.StreamLoadState;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditEvent.EventType;
import org.apache.doris.plugin.StreamLoadAuditEvent;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStreamLoadRecord;
import org.apache.doris.thrift.TStreamLoadRecordResult;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
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

import com.google.gson.annotations.SerializedName;

public class StreamLoadRecordMgr extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(StreamLoadRecordMgr.class);

    private class StreamLoadItem {
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
                String de_label = record.getLabel();
                long de_dbId = record.getDbId();

                Map<String, StreamLoadRecord> labelToStreamLoadRecord = dbIdToLabelToStreamLoadRecord.get(de_dbId);
                Iterator<Map.Entry<String, StreamLoadRecord>> iter_record = labelToStreamLoadRecord.entrySet().iterator();
                while (iter_record.hasNext()) {
                    String labelInMap = iter_record.next().getKey();
                    if (labelInMap.equals(de_label)) {
                        iter_record.remove();
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
        }
        writeUnlock();
    }

    public List<List<Comparable>> getStreamLoadRecordByDb(long dbId, String label, boolean accurateMatch, StreamLoadState state) {
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

    @Override
    protected void runAfterCatalogReady() {
        ImmutableMap<Long, Backend> backends = Catalog.getCurrentSystemInfo().getIdToBackend();
        long start = System.currentTimeMillis();
        int pullRecordSize = 0;
        Map<Long, Long> beIdToLastStreamLoad = Maps.newHashMap();
        for (Backend backend : backends.values()) {
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
                    TStreamLoadRecord streamLoadItem= entry.getValue();
                    String startTime = TimeUtils.longToTimeString(streamLoadItem.getStartTime(), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
                    String finishTime = TimeUtils.longToTimeString(streamLoadItem.getFinishTime(), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("receive stream load record info from backend: {}. label: {}, db: {}, tbl: {}, user: {}, user_ip: {}," +
                                        " status: {}, message: {}, error_url: {}, total_rows: {}, loaded_rows: {}, filtered_rows: {}," +
                                        " unselected_rows: {}, load_bytes: {}, start_time: {}, finish_time: {}.",
                                backend.getHost(), streamLoadItem.getLabel(), streamLoadItem.getDb(), streamLoadItem.getTbl(), streamLoadItem.getUser(), streamLoadItem.getUserIp(),
                                streamLoadItem.getStatus(), streamLoadItem.getMessage(), streamLoadItem.getUrl(), streamLoadItem.getTotalRows(), streamLoadItem.getLoadedRows(),
                                streamLoadItem.getFilteredRows(), streamLoadItem.getUnselectedRows(), streamLoadItem.getLoadBytes(), startTime, finishTime);
                    }

                    AuditEvent auditEvent = new StreamLoadAuditEvent.AuditEventBuilder().setEventType(EventType.STREAM_LOAD_FINISH)
                            .setLabel(streamLoadItem.getLabel()).setDb(streamLoadItem.getDb()).setTable(streamLoadItem.getTbl())
                            .setUser(streamLoadItem.getUser()).setClientIp(streamLoadItem.getUserIp()).setStatus(streamLoadItem.getStatus())
                            .setMessage(streamLoadItem.getMessage()).setUrl(streamLoadItem.getUrl()).setTotalRows(streamLoadItem.getTotalRows())
                            .setLoadedRows(streamLoadItem.getLoadedRows()).setFilteredRows(streamLoadItem.getFilteredRows())
                            .setUnselectedRows(streamLoadItem.getUnselectedRows()).setLoadBytes(streamLoadItem.getLoadBytes())
                            .setStartTime(startTime).setFinishTime(finishTime).build();
                    Catalog.getCurrentCatalog().getAuditEventProcessor().handleAuditEvent(auditEvent);
                    if (entry.getValue().getFinishTime() > lastStreamLoadTime) {
                        lastStreamLoadTime = entry.getValue().getFinishTime();
                    }

                    if (Config.disable_show_stream_load) {
                        continue;
                    }
                    StreamLoadRecord streamLoadRecord = new StreamLoadRecord(streamLoadItem.getLabel(), streamLoadItem.getDb(), streamLoadItem.getTbl(),
                            streamLoadItem.getUser(), streamLoadItem.getUserIp(), streamLoadItem.getStatus(), streamLoadItem.getMessage(), streamLoadItem.getUrl(),
                            String.valueOf(streamLoadItem.getTotalRows()), String.valueOf(streamLoadItem.getLoadedRows()),
                            String.valueOf(streamLoadItem.getFilteredRows()), String.valueOf(streamLoadItem.getUnselectedRows()),
                            String.valueOf(streamLoadItem.getLoadBytes()), startTime, finishTime);

                    String cluster = streamLoadItem.getCluster();
                    if (Strings.isNullOrEmpty(cluster)) {
                        cluster = SystemInfoService.DEFAULT_CLUSTER;
                    }

                    String fullDbName = ClusterNamespace.getFullName(cluster, streamLoadItem.getDb());
                    Database db = Catalog.getCurrentCatalog().getDb(fullDbName);
                    if (db == null) {
                        String dbName = fullDbName;
                        if (Strings.isNullOrEmpty(streamLoadItem.getCluster())) {
                            dbName = streamLoadItem.getDb();
                        }
                        throw new UserException("unknown database, database=" + dbName);
                    }
                    long dbId = db.getId();
                    Catalog.getCurrentCatalog().getStreamLoadRecordMgr().addStreamLoadRecord(dbId, streamLoadItem.getLabel(), streamLoadRecord);
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
            Catalog.getCurrentCatalog().getEditLog().logFetchStreamLoadRecord(fetchStreamLoadRecord);
        }

        if (Config.disable_show_stream_load) {
            Catalog.getCurrentCatalog().getStreamLoadRecordMgr().clearStreamLoadRecord();
        }
    }

    public void replayFetchStreamLoadRecord(FetchStreamLoadRecord fetchStreamLoadRecord) {
        ImmutableMap<Long, Backend> backends = Catalog.getCurrentSystemInfo().getIdToBackend();
        Map<Long, Long> beIdToLastStreamLoad = fetchStreamLoadRecord.getBeIdToLastStreamLoad();
        for (Backend backend : backends.values()) {
            if (beIdToLastStreamLoad.containsKey(backend.getId())) {
                long lastStreamLoadTime = beIdToLastStreamLoad.get(backend.getId());
                LOG.info("Replay stream load bdbje. backend: {}, last stream load time: {}", backend.getHost(), lastStreamLoadTime);
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
