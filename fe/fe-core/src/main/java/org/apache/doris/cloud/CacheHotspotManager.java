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

package org.apache.doris.cloud;

import org.apache.doris.analysis.CancelCloudWarmUpStmt;
import org.apache.doris.analysis.WarmUpClusterStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.CloudWarmUpJob.JobState;
import org.apache.doris.cloud.CloudWarmUpJob.JobType;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TGetTopNHotPartitionsRequest;
import org.apache.doris.thrift.TGetTopNHotPartitionsResponse;
import org.apache.doris.thrift.THotPartition;
import org.apache.doris.thrift.THotTableMessage;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CacheHotspotManager extends MasterDaemon {
    public static final int MAX_SHOW_ENTRIES = 2000;
    private static final Logger LOG = LogManager.getLogger(CacheHotspotManager.class);
    private static final int CYCLE_COUNT_TO_CHECK_EXPIRE_CLOUD_WARM_UP_JOB = 20;
    private static int MAX_ACTIVE_CLOUD_WARM_UP_JOB_SIZE = 10;
    private final CloudSystemInfoService nodeMgr;

    // periodically clear and re-build <id, table> message for
    // efficiency and memory consumption issue
    private Map<Long, Table> idToTable = new HashMap<>();

    private boolean tableCreated = false;

    private List<String> insertValueBatches = new ArrayList<String>();

    private int cycleCount = 0;

    private MasterDaemon jobDaemon;

    private boolean startJobDaemon = false;

    private ConcurrentMap<Long, CloudWarmUpJob> cloudWarmUpJobs = Maps.newConcurrentMap();

    private ConcurrentMap<Long, CloudWarmUpJob> activeCloudWarmUpJobs = Maps.newConcurrentMap();

    private ConcurrentMap<Long, CloudWarmUpJob> runnableCloudWarmUpJobs = Maps.newConcurrentMap();

    private Set<String> runnableClusterSet = ConcurrentHashMap.newKeySet();

    private final ThreadPoolExecutor cloudWarmUpThreadPool = ThreadPoolManager.newDaemonCacheThreadPool(
            MAX_ACTIVE_CLOUD_WARM_UP_JOB_SIZE, "cloud-warm-up-pool", true);

    public CacheHotspotManager(CloudSystemInfoService nodeMgr) {
        super("CacheHotspotManager", Config.fetch_cluster_cache_hotspot_interval_ms);
        this.nodeMgr = nodeMgr;
    }

    @Override
    public void runAfterCatalogReady() {
        if (!startJobDaemon) {
            jobDaemon = new JobDaemon();
            jobDaemon.start();
            startJobDaemon = true;
        }
        if (!tableCreated) {
            try {
                CacheHotspotManagerUtils.execCreateCacheTable();
                tableCreated = true;
            } catch (Exception e) {
                LOG.warn("Create cache hot spot table failed", e);
                return;
            }
        }
        traverseAllDatabaseForTable();
        // it's thread safe to iterate through this concurrent map's ref
        nodeMgr.getCloudClusterIdToBackend().entrySet().forEach(clusterToBeList -> {
            List<Pair<CompletableFuture<TGetTopNHotPartitionsResponse>, Backend>> futureList
                    = new ArrayList<>();
            clusterToBeList.getValue().forEach(backend -> {
                try {
                    futureList.add(getTopNHotPartitionsAsync(backend));
                } catch (TException | RpcException e) {
                    LOG.warn("send getTopNHotPartitionsAsync to be {} failed due to {}", backend, e);
                }
            });

            List<Pair<TGetTopNHotPartitionsResponse, Backend>> responseList = fetchOneClusterHotSpot(futureList);
            responseList.forEach((Pair<TGetTopNHotPartitionsResponse, Backend> respPair) -> {
                TGetTopNHotPartitionsResponse resp = respPair.first;
                if (resp.isSetHotTables()) {
                    resp.getHotTables().forEach((THotTableMessage hotTable) -> {
                        if (hotTable.isSetHotPartitions()) {
                            hotTable.hot_partitions.forEach((THotPartition partition) -> {
                                insertIntoTable(clusterToBeList.getKey(), hotTable.table_id,
                                        hotTable.index_id, resp.file_cache_size, partition, respPair.second);
                            });
                        }
                    });
                }
                triggerBatchInsert();
            });
        });
        idToTable.clear();
    }

    public boolean containsCluster(String clusterName) {
        return CacheHotspotManagerUtils.clusterContains(nodeMgr.getCloudClusterIdByName(clusterName));
    }

    // table_id table_name, index_id, partition_id
    public List<List<String>> getClusterTopNHotPartitions(String clusterName) {
        return CacheHotspotManagerUtils.getClusterTopNPartitions(nodeMgr.getCloudClusterIdByName(clusterName));
    }

    /**
     * traverse all database to cache all tableId -> table
     */
    private void traverseAllDatabaseForTable() {
        // dbs are stored in one concurrent hash map in catalog
        // return one list of snapshot, the item might be deleted
        // but java guarantee it will not be erased from memory
        Env.getCurrentInternalCatalog().getDbs().forEach(database -> {
            try {
                database.readLock();
                // database already dropped
                if (database.getDbState() != Database.DbState.NORMAL) {
                    return;
                }
                // it's thread safe to merge one concurrent map
                idToTable.putAll(database.getIdToTableRef());
            } finally {
                database.readUnlock();
            }
        });
    }

    private void triggerBatchInsert() {
        try {
            CacheHotspotManagerUtils.doBatchInsert(insertValueBatches);
        } catch (Exception e) {
            LOG.warn("Failed to insert into file cache hotspot table due to ", e);
        } finally {
            insertValueBatches.clear();
        }
    }

    private void insertIntoTable(String clusterId, long tableId, long indexId, long fileCacheSize,
            THotPartition partition, Backend backend) {
        LOG.info("table id {}, index id {}, partition id {}", tableId, indexId, partition.partition_id);
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
        DateTimeFormatter dateformatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDateTime = now.format(formatter);
        String insertDay = now.format(dateformatter);

        Map<String, String> params = new HashMap<>();
        params.put("cluster_id", clusterId);
        params.put("cluster_name", nodeMgr.getClusterNameByClusterId(clusterId));
        params.put("backend_id", String.valueOf(backend.getId()));
        params.put("creation_time", formattedDateTime);
        params.put("file_cache_size", String.valueOf(fileCacheSize));
        params.put("insert_day", insertDay);
        Table t;
        if (!idToTable.containsKey(tableId)) {
            return;
        }
        // it might be null?
        t = idToTable.get(tableId);
        params.put("table_id", String.valueOf(tableId));
        params.put("table_name", String.format("%s.%s", t.getDBName(), t.getName()));
        OlapTable olapTable = (OlapTable) t;
        params.put("index_name", String.valueOf(olapTable.getIndexNameById(indexId)));
        params.put("index_id", String.valueOf(indexId));
        Optional<Partition> op = t.getPartitionNames().stream().map(t::getPartition)
                                .filter(p -> p.getId() == partition.partition_id).findAny();
        if (!op.isPresent()) {
            LOG.warn("partition id {} is invalid", partition.partition_id);
            return;
        }
        params.put("partition_name", op.get().getName());
        params.put("partition_id", String.valueOf(partition.partition_id));
        LOG.info("has qpd {}, has qpw {}", partition.isSetQueryPerDay(), partition.isSetQueryPerWeek());
        if (partition.isSetQueryPerDay()) {
            params.put("qpd", String.valueOf(partition.getQueryPerDay()));
        } else {
            params.put("qpd", "0");
        }
        if (partition.isSetQueryPerWeek()) {
            params.put("qpw", String.valueOf(partition.getQueryPerWeek()));
        } else {
            params.put("qpw", "0");
        }
        // Doris's datetime v2 doesn't support time zone
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(partition.last_access_time),
                ZoneId.systemDefault());
        params.put("last_access_time", localDateTime.format(formatter));

        CacheHotspotManagerUtils.transformIntoCacheHotSpotTableValue(params, insertValueBatches);
        if (insertValueBatches.size() == Config.batch_insert_cluster_cache_hotspot_num) {
            triggerBatchInsert();
        }
    }

    private Pair<CompletableFuture<TGetTopNHotPartitionsResponse>, Backend>
            getTopNHotPartitionsAsync(Backend be) throws TException, RpcException, RuntimeException {
        CompletableFuture<TGetTopNHotPartitionsResponse> f = CompletableFuture.supplyAsync(() -> {
            boolean ok = false;
            BackendService.Client client = null;
            TNetworkAddress address = null;
            try {
                address = new TNetworkAddress(be.getHost(), be.getBePort());
                client = ClientPool.backendPool.borrowObject(address);
                TGetTopNHotPartitionsResponse resp = client.getTopNHotPartitions(
                    new TGetTopNHotPartitionsRequest());
                ok = true;
                return resp;
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                if (ok) {
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    ClientPool.backendPool.invalidateObject(address, client);
                }
            }
        });
        return Pair.of(f, be);
    }

    // the message we need:
    // cluster_id varchar,
    // cluster_name varchar,
    // backend_id bigint,
    // creation_time DATETIMEV2,
    // file_cache_size bigint,
    // table_name varchar,
    // partition_name varchar,
    // last_access_time DATETIMEV2
    private List<Pair<TGetTopNHotPartitionsResponse, Backend>> fetchOneClusterHotSpot(
            List<Pair<CompletableFuture<TGetTopNHotPartitionsResponse>, Backend>> futureList) {
        List<Pair<TGetTopNHotPartitionsResponse, Backend>> responseList = new ArrayList<>();
        long timeoutMs = Math.min(5000, Config.remote_fragment_exec_timeout_ms);
        futureList.forEach(futureBackendPair -> {
            TStatusCode code = TStatusCode.OK;
            String errMsg = null;
            Exception exception = null;
            Future<TGetTopNHotPartitionsResponse> f = futureBackendPair.key();
            try {
                // temporary value， would change to config
                TGetTopNHotPartitionsResponse result = f.get(timeoutMs, TimeUnit.MILLISECONDS);
                responseList.add(Pair.of(result, futureBackendPair.second));
            } catch (ExecutionException e) {
                exception = e;
                code = TStatusCode.THRIFT_RPC_ERROR;
            } catch (InterruptedException e) {
                exception = e;
                code = TStatusCode.INTERNAL_ERROR;
            } catch (TimeoutException e) {
                exception = e;
                errMsg = "timeout when waiting for fetch cache hotspot RPC. Wait(sec): " + timeoutMs / 1000;
                code = TStatusCode.TIMEOUT;
            }
            if (code != TStatusCode.OK) {
                LOG.warn("Fetch be {}'s cache hotspot information throw {}, errmsg {}",
                        futureBackendPair.second.getAddress(), exception, errMsg);
            }
        });
        return responseList;
    }

    private Long getFileCacheUsedBytes(String clusterName) throws RuntimeException {
        List<Backend> backends = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                                        .getBackendsByClusterName(clusterName);
        Long totalFileCache = 0L;
        for (Backend backend : backends) {
            Long fileCacheSize = 0L;
            boolean ok = false;
            BackendService.Client client = null;
            TNetworkAddress address = null;
            try {
                address = new TNetworkAddress(backend.getHost(), backend.getBePort());
                client = ClientPool.backendPool.borrowObject(address);
                TGetTopNHotPartitionsResponse resp = client.getTopNHotPartitions(
                        new TGetTopNHotPartitionsRequest());
                fileCacheSize = resp.file_cache_size;
                ok = true;
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                if (ok) {
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    ClientPool.backendPool.invalidateObject(address, client);
                }
            }
            totalFileCache += fileCacheSize;
        }
        return totalFileCache;
    }

    private Map<Long, List<Tablet>> warmUpNewClusterByTable(String dstClusterName, String dbName, String tableName,
                                            String partitionName, boolean isForce) throws RuntimeException {
        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbName);
        OlapTable table = (OlapTable) db.getTableNullable(tableName);
        List<Partition> partitions = new ArrayList<>();
        if (partitionName.length() != 0) {
            partitions.add(table.getPartition(partitionName));
        } else {
            partitions.addAll(table.getPartitions());
        }
        List<Backend> backends = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                                        .getBackendsByClusterName(dstClusterName);
        Long totalFileCache = getFileCacheUsedBytes(dstClusterName);
        Long warmUpTotalFileCache = 0L;
        List<Partition> warmUpPartitions = new ArrayList<>();
        for (Partition partition : partitions) {
            warmUpTotalFileCache += partition.getDataSize(true);
            warmUpPartitions.add(partition);
            if (warmUpTotalFileCache > totalFileCache) {
                if (!isForce) {
                    throw new RuntimeException("The cluster " + dstClusterName + "file cache size is not enough");
                } else {
                    break;
                }
            }
        }
        List<MaterializedIndex> indexes = new ArrayList<>();
        for (Partition partition : warmUpPartitions) {
            indexes.addAll(partition.getMaterializedIndices(IndexExtState.VISIBLE));
        }
        List<Tablet> tablets = new ArrayList<>();
        for (MaterializedIndex index : indexes) {
            tablets.addAll(index.getTablets());
        }
        Map<Long, List<Tablet>> beToWarmUpTablets = new HashMap<>();
        for (Backend backend : backends) {
            Set<Long> beTabletIds = ((CloudEnv) Env.getCurrentEnv())
                                    .getCloudTabletRebalancer()
                                    .getSnapshotTabletsByBeId(backend.getId());
            List<Tablet> warmUpTablets = new ArrayList<>();
            for (Tablet tablet : tablets) {
                if (beTabletIds.contains(tablet.getId())) {
                    warmUpTablets.add(tablet);
                }
            }
            beToWarmUpTablets.put(backend.getId(), warmUpTablets);
        }
        return beToWarmUpTablets;
    }

    private Map<Long, List<List<Long>>> splitBatch(Map<Long, List<Tablet>> beToWarmUpTablets) {
        final Long maxSizePerBatch = 10737418240L; // 10G
        Map<Long, List<List<Long>>> beToTabletIdBatches = new HashMap<>();
        for (Map.Entry<Long, List<Tablet>> entry : beToWarmUpTablets.entrySet()) {
            List<List<Long>> batches = new ArrayList<>();
            List<Long> batch = new ArrayList<>();
            Long curBatchSize = 0L;
            for (Tablet tablet : entry.getValue()) {
                if (curBatchSize + tablet.getDataSize(true) > maxSizePerBatch) {
                    batches.add(batch);
                    batch = new ArrayList<>();
                    curBatchSize = 0L;
                }
                batch.add(tablet.getId());
                curBatchSize += tablet.getDataSize(true);
            }
            if (!batch.isEmpty()) {
                batches.add(batch);
            }
            beToTabletIdBatches.put(entry.getKey(), batches);
        }
        return beToTabletIdBatches;
    }

    private Map<Long, List<Tablet>> warmUpNewClusterByCluster(String dstClusterName, String srcClusterName) {
        Long dstTotalFileCache = getFileCacheUsedBytes(dstClusterName);
        List<List<String>> result = getClusterTopNHotPartitions(srcClusterName);
        Long warmUpTabletsSize = 0L;
        List<Tablet> tablets = new ArrayList<>();
        for (List<String> line : result) {
            Long tableId = Long.parseLong(line.get(0));
            String[] tmp = line.get(1).split("\\.");
            String dbName = tmp[0];
            Long partitionId = Long.parseLong(line.get(2));
            Long indexId = Long.parseLong(line.get(3));
            Database db = Env.getCurrentInternalCatalog().getDbNullable("default_cluster:" + dbName);
            if (db == null) {
                continue;
            }
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            if (table == null) {
                continue;
            }
            Partition partition = table.getPartition(partitionId);
            if (partition == null) {
                continue;
            }
            MaterializedIndex index = partition.getIndex(indexId);
            if (index == null) {
                continue;
            }
            for (Tablet tablet : index.getTablets()) {
                warmUpTabletsSize += tablet.getDataSize(true);
                tablets.add(tablet);
                if (warmUpTabletsSize >= dstTotalFileCache) {
                    break;
                }
            }
            if (warmUpTabletsSize >= dstTotalFileCache) {
                break;
            }
        }
        Collections.reverse(tablets);
        List<Backend> backends = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                                        .getBackendsByClusterName(dstClusterName);
        Map<Long, List<Tablet>> beToWarmUpTablets = new HashMap<>();
        for (Backend backend : backends) {
            Set<Long> beTabletIds = ((CloudEnv) Env.getCurrentEnv())
                                    .getCloudTabletRebalancer()
                                    .getSnapshotTabletsByBeId(backend.getId());
            List<Tablet> warmUpTablets = new ArrayList<>();
            for (Tablet tablet : tablets) {
                if (beTabletIds.contains(tablet.getId())) {
                    warmUpTablets.add(tablet);
                }
            }
            beToWarmUpTablets.put(backend.getId(), warmUpTablets);
        }
        return beToWarmUpTablets;
    }

    public List<List<String>> getSingleJobInfo(long jobId) throws AnalysisException {
        List<List<String>> infos = new ArrayList<List<String>>();
        CloudWarmUpJob job = cloudWarmUpJobs.get(jobId);
        if (job == null) {
            throw new AnalysisException("cloud warm up with job " + jobId + " does not exist");
        }
        infos.add(job.getJobInfo());
        return infos;
    }

    private class JobDaemon extends MasterDaemon {
        JobDaemon() {
            super("JobDaemon", Config.cloud_warm_up_job_scheduler_interval_millisecond);
            LOG.info("start cloud warm up job daemon");
        }

        @Override
        public void runAfterCatalogReady() {
            if (cycleCount >= CYCLE_COUNT_TO_CHECK_EXPIRE_CLOUD_WARM_UP_JOB) {
                clearFinishedOrCancelCloudWarmUpJob();
                cycleCount = 0;
            }
            ++cycleCount;
            runCloudWarmUpJob();
        }
    }

    private void clearFinishedOrCancelCloudWarmUpJob() {
        Iterator<Entry<Long, CloudWarmUpJob>> iterator = runnableCloudWarmUpJobs.entrySet().iterator();
        while (iterator.hasNext()) {
            CloudWarmUpJob cloudWarmUpJob = iterator.next().getValue();
            if (cloudWarmUpJob.isDone()) {
                iterator.remove();
            }
        }
        Iterator<Map.Entry<Long, CloudWarmUpJob>> iterator2 = cloudWarmUpJobs.entrySet().iterator();
        while (iterator2.hasNext()) {
            CloudWarmUpJob cloudWarmUpJob = iterator2.next().getValue();
            if (cloudWarmUpJob.isExpire()) {
                cloudWarmUpJob.setJobState(JobState.DELETED);
                Env.getCurrentEnv().getEditLog().logModifyCloudWarmUpJob(cloudWarmUpJob);
                iterator2.remove();
                LOG.info("remove expired cloud warm up job {}. finish at {}",
                        cloudWarmUpJob.getJobId(), TimeUtils.longToTimeString(cloudWarmUpJob.getFinishedTimeMs()));
            }
        }
    }

    public Map<Long, CloudWarmUpJob> getCloudWarmUpJobs() {
        return this.cloudWarmUpJobs;
    }

    public Set<String> getRunnableClusterSet() {
        return this.runnableClusterSet;
    }

    public List<List<String>> getAllJobInfos(int limit) {
        List<List<String>> infos = Lists.newArrayList();
        Collection<CloudWarmUpJob> allJobs = cloudWarmUpJobs.values();
        allJobs.stream().sorted(Comparator.comparing(CloudWarmUpJob::getCreateTimeMs).reversed())
                        .limit(limit).forEach(t -> {
                            infos.add(t.getJobInfo());
                        });
        return infos;
    }

    public void addCloudWarmUpJob(CloudWarmUpJob job) {
        cloudWarmUpJobs.put(job.getJobId(), job);
        LOG.info("add cloud warm up job {}", job.getJobId());
        runnableCloudWarmUpJobs.put(job.getJobId(), job);
        if (!job.isDone()) {
            runnableClusterSet.add(job.getCloudClusterName());
        }
    }

    private Map<Long, List<Tablet>> warmUpNewClusterByTable(long jobId, String dstClusterName,
            List<Triple<String, String, String>> tables,
            boolean isForce) throws RuntimeException {
        Map<Long, List<Tablet>> beToWarmUpTablets = new HashMap<>();
        Long totalFileCache = getFileCacheUsedBytes(dstClusterName);
        Long warmUpTotalFileCache = 0L;
        for (Triple<String, String, String> tableTriple : tables) {
            if (warmUpTotalFileCache > totalFileCache) {
                break;
            }
            String dbName = tableTriple.getLeft();
            String tableName = tableTriple.getMiddle();
            String partitionName = tableTriple.getRight();
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbName);
            OlapTable table = (OlapTable) db.getTableNullable(tableName);
            List<Partition> partitions = new ArrayList<>();
            if (partitionName.length() != 0) {
                partitions.add(table.getPartition(partitionName));
            } else {
                partitions.addAll(table.getPartitions());
            }
            List<Backend> backends = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                                            .getBackendsByClusterName(dstClusterName);
            List<Partition> warmUpPartitions = new ArrayList<>();
            for (Partition partition : partitions) {
                warmUpTotalFileCache += partition.getDataSize(true);
                warmUpPartitions.add(partition);
                if (warmUpTotalFileCache > totalFileCache) {
                    break;
                }
            }
            List<MaterializedIndex> indexes = new ArrayList<>();
            for (Partition partition : warmUpPartitions) {
                indexes.addAll(partition.getMaterializedIndices(IndexExtState.VISIBLE));
            }
            List<Tablet> tablets = new ArrayList<>();
            for (MaterializedIndex index : indexes) {
                tablets.addAll(index.getTablets());
            }
            for (Backend backend : backends) {
                Set<Long> beTabletIds = ((CloudEnv) Env.getCurrentEnv())
                                        .getCloudTabletRebalancer()
                                        .getSnapshotTabletsByBeId(backend.getId());
                List<Tablet> warmUpTablets = new ArrayList<>();
                for (Tablet tablet : tablets) {
                    if (beTabletIds.contains(tablet.getId())) {
                        warmUpTablets.add(tablet);
                    }
                }
                beToWarmUpTablets.computeIfAbsent(backend.getId(),
                        k -> new ArrayList<>()).addAll(warmUpTablets);
            }
        }
        LOG.info("The job {} warm up size is {}, the cluster cache size is {}",
                    jobId, warmUpTotalFileCache, totalFileCache);
        if (warmUpTotalFileCache > totalFileCache && !isForce) {
            throw new RuntimeException("The cluster " + dstClusterName + " cache size is not enough");
        }
        return beToWarmUpTablets;
    }

    public long createJob(WarmUpClusterStmt stmt) throws AnalysisException {
        if (runnableClusterSet.contains(stmt.getDstClusterName())) {
            throw new AnalysisException("cluster: " + stmt.getDstClusterName() + " already has a runnable job");
        }
        Map<Long, List<Tablet>> beToWarmUpTablets = new HashMap<>();
        long jobId = Env.getCurrentEnv().getNextId();
        if (!FeConstants.runningUnitTest) {
            if (stmt.isWarmUpWithTable()) {
                beToWarmUpTablets = warmUpNewClusterByTable(jobId, stmt.getDstClusterName(), stmt.getTables(),
                                                            stmt.isForce());
            } else {
                beToWarmUpTablets = warmUpNewClusterByCluster(stmt.getDstClusterName(), stmt.getSrcClusterName());
            }
        }

        Map<Long, List<List<Long>>> beToTabletIdBatches = splitBatch(beToWarmUpTablets);

        CloudWarmUpJob.JobType jobType = stmt.isWarmUpWithTable() ? JobType.TABLE : JobType.CLUSTER;
        CloudWarmUpJob warmUpJob = new CloudWarmUpJob(jobId, stmt.getDstClusterName(), beToTabletIdBatches, jobType);
        addCloudWarmUpJob(warmUpJob);

        Env.getCurrentEnv().getEditLog().logModifyCloudWarmUpJob(warmUpJob);
        LOG.info("finished to create cloud warm up job: {}", warmUpJob.getJobId());

        return jobId;

    }

    public void cancel(CancelCloudWarmUpStmt stmt) throws DdlException {
        CloudWarmUpJob job = cloudWarmUpJobs.get(stmt.getJobId());
        if (job == null) {
            throw new DdlException("job id: " + stmt.getJobId() + " does not exist.");
        }
        if (!job.cancel("user cancel")) {
            throw new DdlException("job can not be cancelled. State: " + job.getJobState());
        }
    }

    private void runCloudWarmUpJob() {
        runnableCloudWarmUpJobs.values().forEach(cloudWarmUpJob -> {
            if (!cloudWarmUpJob.isDone() && !activeCloudWarmUpJobs.containsKey(cloudWarmUpJob.getJobId())
                    && activeCloudWarmUpJobs.size() < MAX_ACTIVE_CLOUD_WARM_UP_JOB_SIZE) {
                if (FeConstants.runningUnitTest) {
                    cloudWarmUpJob.run();
                } else {
                    cloudWarmUpThreadPool.submit(() -> {
                        if (activeCloudWarmUpJobs.putIfAbsent(cloudWarmUpJob.getJobId(), cloudWarmUpJob) == null) {
                            try {
                                cloudWarmUpJob.run();
                            } finally {
                                activeCloudWarmUpJobs.remove(cloudWarmUpJob.getJobId());
                            }
                        }
                    });
                }
            }
        });
    }

    public void replayCloudWarmUpJob(CloudWarmUpJob cloudWarmUpJob) throws Exception {
        // ATTN: not need to replay, just override the job with the same job id.
        runnableCloudWarmUpJobs.put(cloudWarmUpJob.getJobId(), cloudWarmUpJob);
        cloudWarmUpJobs.put(cloudWarmUpJob.getJobId(), cloudWarmUpJob);
        LOG.info("replay cloud warm up job {}, state {}", cloudWarmUpJob.getJobId(), cloudWarmUpJob.getJobState());
        if (cloudWarmUpJob.isDone()) {
            runnableClusterSet.remove(cloudWarmUpJob.getCloudClusterName());
        } else {
            runnableClusterSet.add(cloudWarmUpJob.getCloudClusterName());
        }
        if (cloudWarmUpJob.jobState == JobState.DELETED) {
            if (cloudWarmUpJobs.remove(cloudWarmUpJob.getJobId()) != null
                    && runnableCloudWarmUpJobs.remove(cloudWarmUpJob.getJobId()) != null) {
                LOG.info("replay removing expired cloud warm up job {}.", cloudWarmUpJob.getJobId());
            } else {
                // should not happen, but it does no matter, just add a warn log here to observe
                LOG.warn("failed to find cloud warm up job {} when replay removing expired job.",
                            cloudWarmUpJob.getJobId());
            }
        }
    }

}
