package org.apache.doris.clone;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.CloneTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/*
 * TabletFactory saved the tablets produced by TabletScanner and try to repair them.
 * It also try to balance the cluster load if there is no tablet need to be repaired.
 * 
 * We are expecting to an efficient way to recovery the entire cluster and make it balanced.
 * Case 1:
 *  A Backend is down. All tablets which has replica on this BE should be repaired as soon as possible.
 *  
 * Case 1.1:
 *  As Backend is down, some tables should be repaired in high priority. So the repair task should be able
 *  to preempted.
 *  
 * Case 2:
 *  A new Backend is added to the cluster. Replicas should be transfer to that host to balance the cluster load.
 * 1 sec
 */
public class TabletFactory extends Daemon {
    private static final Logger LOG = LogManager.getLogger(TabletFactory.class);

    private PriorityBlockingQueue<TabletInfo> q = new PriorityBlockingQueue<>();;
    private Set<Long> runningTabletIds = Sets.newHashSet();;

    /*
     *  this is a init #working slot per backend.
     *  We will increase or decrease the slot num dynamically based on the clone task statistic info
     */
    private static int BATCH_NUM = 10; // handle at most BATCH_NUM tablets in one loop

    private static int SLOT_NUM_PER_PATH = 1;

    // be id -> #working slots
    private Map<Long, Slot> backendsWorkingSlots = Maps.newConcurrentMap();;
    
    private Catalog catalog;
    private SystemInfoService infoService;
    private TabletInvertedIndex invertedIndex;

    // cluster name -> load statistic
    private Map<String, ClusterLoadStatistic> statisticMap = Maps.newConcurrentMap();
    private long lastLoadStatisticUpdateTime = 0;
    private static long UPDATE_INTERVAL_MS = 60 * 1000;

    private boolean isInit = false;

    public TabletFactory() {
        super("tablet factory", 1000);
        catalog = Catalog.getCurrentCatalog();
        infoService = Catalog.getCurrentSystemInfo();
        invertedIndex = Catalog.getCurrentInvertedIndex();
    }

    public boolean init() {
        return initWorkingSlots();
    }

    private boolean initWorkingSlots() {
        ImmutableMap<Long, Backend> backends = infoService.getBackendsInCluster(null);
        for (Backend be : backends.values()) {
            List<Long> pathHashes = be.getDisks().values().stream().map(v -> v.getPathHash()).collect(Collectors.toList());
            Slot slot = new Slot(pathHashes, SLOT_NUM_PER_PATH);
            backendsWorkingSlots.put(be.getId(), slot);
            LOG.info("init backend {} working slots: {}", be.getId(), be.getDisks().size());
        }

        // TODO(cmy): the path hash in DiskInfo may not be available before the entire cluster being upgraded
        // to the latest version.
        // So we have to wait until we get all path hash info.
        return false;
    }

    public void updateWorkingSlots() {
        ImmutableMap<Long, Backend> backends = infoService.getBackendsInCluster(null);
        Set<Long> deletedBeIds = Sets.newHashSet();

        // update exist backends
        for (Long beId : backendsWorkingSlots.keySet()) {
            if (backends.containsKey(beId)) {
                List<Long> pathHashes = backends.get(beId).getDisks().values().stream().map(v -> v.getPathHash()).collect(Collectors.toList());
                backendsWorkingSlots.get(beId).updateSlots(pathHashes);
            } else {
                deletedBeIds.add(beId);
            }
        }

        // delete non-exist backends
        for (Long beId : deletedBeIds) {
            backendsWorkingSlots.remove(beId);
        }

        // add new backends
        for (Backend be : backends.values()) {
            if (!backendsWorkingSlots.containsKey(be.getId())) {
                List<Long> pathHashes = be.getDisks().values().stream().map(v -> v.getPathHash()).collect(Collectors.toList());
                Slot slot = new Slot(pathHashes, SLOT_NUM_PER_PATH);
                backendsWorkingSlots.put(be.getId(), slot);
                LOG.info("init backend {} working slots: {}", be.getId(), be.getDisks().size());
            }
        }
    }

    public void addNewBackend(long newBackendId) {
        // backendsWorkingSlots.putIfAbsent(newBackendId, INIT_BACKEND_WORKING_SLOT);
    }

    public void deleteBackend(long backendId) {
        backendsWorkingSlots.remove(backendId);
    }

    public Map<Long, Slot> getBackendsWorkingSlots() {
        return backendsWorkingSlots;
    }

    public synchronized boolean addTablet(TabletInfo tablet) {
        if (runningTabletIds.contains(tablet.getTabletId())) {
            return false;
        }
        runningTabletIds.add(tablet.getTabletId());
        q.offer(tablet);
        return true;
    }

    public boolean isEmpty() {
        return q.isEmpty();
    }

    /*
     * TabletFactory will run as a daemon thread at a very short interval(default 1 sec)
     * It will try to repair the tablet in queue, and try to balance the cluster if possible.
     */
    @Override
    protected void runOneCycle() {
        if (!isInit && !init()) {
            // not ready to start.
            return;
        }

        updateClusterLoadStatisticsIfNecessary();

        handleQ();

        doBalance();
    }

    /*
     * Update cluster load statistic info in a reasonable interval.
     * Here is the only place we update the cluster load statistic info.
     * We will not update this info dynamically along with the clone job's running.
     * Although it will cause a little bit inaccurate, but is within a controllable range,
     * because we already limit the total number of running clone jobs in cluster by 'backend slots'
     */
    private void updateClusterLoadStatisticsIfNecessary() {
        if (System.currentTimeMillis() - lastLoadStatisticUpdateTime < UPDATE_INTERVAL_MS) {
            return;
        }

        statisticMap.clear();
        List<String> clusterNames = infoService.getClusterNames();
        for (String clusterName : clusterNames) {
            ClusterLoadStatistic clusterLoadStatistic = new ClusterLoadStatistic(catalog, infoService,
                    invertedIndex);
            clusterLoadStatistic.init(clusterName);
            statisticMap.put(clusterName, clusterLoadStatistic);
        }
        lastLoadStatisticUpdateTime = System.currentTimeMillis();
    }

    /*
     * get at most BATCH_NUM tablets from queue, and try to repair them.
     */
    private void handleQ() {
        List<TabletInfo> currentBatch = getNextTabletInfoBatch();
        LOG.debug("get {} tablets info to repair", currentBatch.size());

        for (TabletInfo tabletInfo : currentBatch) {
            handleTablet(tabletInfo);
        }
    }

    /*
     * Try to repair a single tablet.
     * All info which need to be cleaned after repairing should put into the tabletInfo, such as slot info, etc...
     * and then put tabletInfo in 'deprecatedTablets'.
     * All tabletInfo in 'deprecatedTablets' will be wiped out finally.
     */
    private void handleTablet(TabletInfo tabletInfo) {
        Database db = catalog.getDb(tabletInfo.getDbId());
        if (db == null) {
            discardTabletInfo(tabletInfo, "db " + tabletInfo.getDbId() + " does not exist");
            return;
        }

        Pair<TabletStatus, TabletInfo.Priority> statusPair = null;
        db.writeLock();
        try {
            // check this tablet again
            OlapTable tbl = (OlapTable) db.getTable(tabletInfo.getTblId());
            if (tbl == null) {
                discardTabletInfo(tabletInfo, "tbl " + tabletInfo.getTblId() + " does not exist");
                return;
            }

            Partition partition = tbl.getPartition(tabletInfo.getPartitionId());
            if (partition == null) {
                discardTabletInfo(tabletInfo, "partition " + tabletInfo.getPartitionId() + " does not exist");
                return;
            }

            MaterializedIndex idx = partition.getIndex(tabletInfo.getIndexId());
            if (idx == null) {
                discardTabletInfo(tabletInfo, "idx " + tabletInfo.getIndexId() + " does not exist");
                return;
            }

            Tablet tablet = idx.getTablet(tabletInfo.getTabletId());
            Preconditions.checkNotNull(tablet);

            statusPair = tablet.getHealthStatusWithPriority(
                                               infoService, tabletInfo.getCluster(),
                                               partition.getCommittedVersion(),
                                               partition.getCommittedVersionHash(),
                                               tbl.getPartitionInfo().getReplicationNum(partition.getId()));

            if (statusPair.first == TabletStatus.HEALTHY) {
                discardTabletInfo(tabletInfo, "tablet is healthy");
                return;
            }

            tabletInfo.setTablet(tablet);
            tabletInfo.setVersionInfo(partition.getVisibleVersion(), partition.getVisibleVersionHash(),
                                      partition.getCommittedVersion(), partition.getCommittedVersionHash());

            handleTabletByStatus(statusPair.first, tabletInfo);

        } finally {
            db.writeUnlock();
        }
    }

    private void handleTabletByStatus(TabletStatus status, TabletInfo tabletInfo) {
        switch (status) {
            case REPLICA_MISSING:
                handleReplicaMissing(tabletInfo);
                break;
            case VERSION_INCOMPLETE:
                handleReplicaVersionIncomplete(tabletInfo);
                break;
            case REDUNDANT:
                handleRedundantReplica(tabletInfo);
                break;
            case REPLICA_MISSING_IN_CLUSTER:
                handleReplicaClusterMigration(tabletInfo);
                break;
            default:
                break;
        }
    }

    /*
     * Replica is missing, which means there is not enough alive replicas.
     * So we need to find a destination backend to clone a new replica as possible as we can.
     * 1. find an available path in a backend as destination:
     *      1. backend need to be alive.
     *      2. backend of existing replicas should be excluded.
     *      3. backend has available slot for clone.
     *      4. replica can fit in the path (consider the threshold of disk capacity and usage percent).
     *      5. try to find a path with lowest load score.
     * 2. find an appropriate source replica:
     *      1. source replica should be healthy
     *      2. backend of source replica has available slot for clone.
     *      
     * 3. send clone task to destination backend
     */
    private void handleReplicaMissing(TabletInfo tabletInfo) {
        // find an available dest backend and path
        RootPathLoadStatistic destPath = chooseAvailableDestPath(tabletInfo);
        if (destPath == null) {
            discardTabletInfo(tabletInfo, "unable to find dest slot for new replica");
            return;
        }
        tabletInfo.setDestination(destPath.getBeId(), destPath.getPathHash());

        // choose a source replica for cloning from
        if (tabletInfo.chooseSrcReplica(backendsWorkingSlots)) {
            discardTabletInfo(tabletInfo, "unable to choose src replica for cloning from");
            return;
        }

        // create and send clone task
        CloneTask task = tabletInfo.createCloneReplicaAndTask();
        if (AgentTaskQueue.addTask(task)) {
            LOG.info("send clone task: {}", tabletInfo);
        }
    }

    /*
     * Replica version is incomplete, which means this replica is missing some version,
     * and need to be cloned from a healthy replica, in-place.
     * 
     * 1. find the incomplete replica as destination replica
     * 2. find a healthy replica as source replica
     * 3. send clone task
     */
    private void handleReplicaVersionIncomplete(TabletInfo tabletInfo) {
        ClusterLoadStatistic statistic = statisticMap.get(tabletInfo.getCluster());
        if (statistic == null) {
            discardTabletInfo(tabletInfo, "cluster " + tabletInfo.getCluster() + " does not exist");
            return;
        }

        if (!tabletInfo.chooseDestReplicaForVersionIncomplete(backendsWorkingSlots)) {
            discardTabletInfo(tabletInfo, "unable to choose dest incomplete replica");
            return;
        }

        if (!tabletInfo.chooseSrcReplicaForVersionIncomplete(backendsWorkingSlots)) {
            discardTabletInfo(tabletInfo, "unable to choose src replica for version incomplete");
            return;
        }

        // create and send clone task
        CloneTask task = tabletInfo.createCloneReplicaAndTask();
        if (AgentTaskQueue.addTask(task)) {
            LOG.info("send clone task: {}", tabletInfo);
        }
    }

    /*
     *  replica is redundant, which means there are more replicas than we expected, which need to be dropped.
     *  we just drop one redundant replica at a time, for safety reason.
     *  choosing a replica to drop base on following priority:
     *  1. backend has been dropped
     *  2. backend is not available
     *  3. replica's state is CLONE
     *  4. replica's last failed version > 0
     *  5. replica with lower version
     *  6. replica not in right cluster
     *  7. replica in higher load backend
     */
    private void handleRedundantReplica(TabletInfo tabletInfo) {
        if (deleteBackendDropped(tabletInfo)
                || deleteBackendUnavailable(tabletInfo)
                || deleteCloneReplica(tabletInfo)
                || deleteReplicaWithFailedVersion(tabletInfo)
                || deleteReplicaWithLowerVersion(tabletInfo)
                || deleteReplicaNotInCluster(tabletInfo)
                || deleteReplicaOnHighLoadBackend(tabletInfo)) {
        }
    }

    private boolean deleteBackendDropped(TabletInfo tabletInfo) {
        for (Replica replica : tabletInfo.getReplicas()) {
            long beId = replica.getBackendId();
            if (infoService.getBackend(beId) == null) {
                deleteReplicaInternal(tabletInfo, replica, "backend dropped");
                return true;
            }
        }
        return false;
    }

    private boolean deleteBackendUnavailable(TabletInfo tabletInfo) {
        for (Replica replica : tabletInfo.getReplicas()) {
            Backend be = infoService.getBackend(replica.getBackendId());
            if (be == null) {
                // this case should be handled in deleteBackendDropped()
                continue;
            }
            if (!be.isAvailable()) {
                deleteReplicaInternal(tabletInfo, replica, "backend unavailable");
                return true;
            }
        }
        return false;
    }

    private boolean deleteCloneReplica(TabletInfo tabletInfo) {
        for (Replica replica : tabletInfo.getReplicas()) {
            if (replica.getState() == ReplicaState.CLONE) {
                deleteReplicaInternal(tabletInfo, replica, "clone state");
                return true;
            }
        }
        return false;
    }

    private boolean deleteReplicaWithFailedVersion(TabletInfo tabletInfo) {
        for (Replica replica : tabletInfo.getReplicas()) {
            if (replica.getLastFailedVersion() > 0) {
                deleteReplicaInternal(tabletInfo, replica, "version incomplete");
                return true;
            }
        }
        return false;
    }

    private boolean deleteReplicaWithLowerVersion(TabletInfo tabletInfo) {
        for (Replica replica : tabletInfo.getReplicas()) {
            if (!replica.checkVersionCatchUp(tabletInfo.getCommittedVersion(), tabletInfo.getCommittedVersionHash())) {
                deleteReplicaInternal(tabletInfo, replica, "lower version");
                return true;
            }
        }
        return false;
    }

    private boolean deleteReplicaNotInCluster(TabletInfo tabletInfo) {
        for (Replica replica : tabletInfo.getReplicas()) {
            Backend be = infoService.getBackend(replica.getBackendId());
            if (be == null) {
                // this case should be handled in deleteBackendDropped()
                continue;
            }
            if (!be.getOwnerClusterName().equals(tabletInfo.getCluster())) {
                deleteReplicaInternal(tabletInfo, replica, "not in cluster");
                return true;
            }
        }
        return false;
    }

    private boolean deleteReplicaOnHighLoadBackend(TabletInfo tabletInfo) {
        ClusterLoadStatistic statistic = statisticMap.get(tabletInfo.getCluster());
        if (statistic == null) {
            return false;
        }
        
        Replica chosenReplica = null;
        double maxScore = 0;
        for (Replica replica : tabletInfo.getReplicas()) {
            BackendLoadStatistic beStatistic = statistic.getBackendLoadStatistic(replica.getBackendId());
            if (beStatistic == null) {
                continue;
            }
            if (beStatistic.getLoadScore() > maxScore) {
                maxScore = beStatistic.getLoadScore();
                chosenReplica = replica;
            }
        }

        if (chosenReplica != null) {
            deleteReplicaInternal(tabletInfo, chosenReplica, "high load");
            return true;
        }
        return false;
    }

    private void deleteReplicaInternal(TabletInfo tabletInfo, Replica replica, String reason) {
        // delete this replica from catalog.
        // it will also delete replica from tablet inverted index.
        tabletInfo.deleteReplica(replica);

        // TODO(cmy): this should be removed after I finish modifying alter job logic
        // Catalog.getInstance().handleJobsWhenDeleteReplica(tabletInfo.getTblId(), tabletInfo.getPartitionId(),
        //                                                   tabletInfo.getIndexId(), tabletInfo.getTabletId(),
        //                                                   replica.getId(), replica.getBackendId());

        // write edit log
        ReplicaPersistInfo info = ReplicaPersistInfo.createForDelete(tabletInfo.getDbId(),
                                                                     tabletInfo.getTblId(),
                                                                     tabletInfo.getPartitionId(),
                                                                     tabletInfo.getIndexId(),
                                                                     tabletInfo.getTabletId(),
                                                                     replica.getBackendId());

        Catalog.getInstance().getEditLog().logDeleteReplica(info);

        LOG.info("delete replica. tablet id: {}, backend id: {}. reason: {}",
                 tabletInfo.getTabletId(), replica.getBackendId(), reason);
    }

    /*
     * Cluster migration, which means the tablet has enough healthy replicas,
     * but some replicas are not in right cluster.
     * It is just same as 'replica missing'.
     * 
     * after clone finished, the replica in wrong cluster will be treated as redundant, and will be deleted soon.
     */
    private void handleReplicaClusterMigration(TabletInfo tabletInfo) {
        handleReplicaMissing(tabletInfo);
    }

    private void doBalance() {
        // TODO(cmy)
    }

    // choose a path on a backend which is fit for the tablet
    private RootPathLoadStatistic chooseAvailableDestPath(TabletInfo tabletInfo) {
        ClusterLoadStatistic statistic = statisticMap.get(tabletInfo.getCluster());
        if (statistic == null) {
            discardTabletInfo(tabletInfo, "cluster " + tabletInfo.getCluster() + " does not exist");
            return null;
        }
        List<BackendLoadStatistic> beStatistics = statistic.getBeLoadStatistics();

        // get all available paths which this tablet can fit in.
        List<RootPathLoadStatistic> allFitPaths = Lists.newArrayList();
        for (int i = 0; i < beStatistics.size(); i++) {
            BackendLoadStatistic bes = beStatistics.get(i);
            // exclude BE which already has replica of this tablet
            if (tabletInfo.containsBE(bes.getBeId())) {
                continue;
            }

            List<RootPathLoadStatistic> resultPaths = Lists.newArrayList();
            BalanceStatus st = bes.isFit(tabletInfo.getTabletSize(), resultPaths, true /* is supplement */);
            if (!st.ok()) {
                LOG.debug("unable to find path for supplementing tablet: {}. {}", tabletInfo, st);
                continue;
            }

            Preconditions.checkState(resultPaths.size() == 1);
            allFitPaths.add(resultPaths.get(0));
        }

        if (allFitPaths.isEmpty()) {
            discardTabletInfo(tabletInfo, "unable to find dest path for new replica");
            return null;
        }

        // all fit paths has already been sorted by load score in 'allFitPaths'.
        // just get first available path.
        for (RootPathLoadStatistic rootPathLoadStatistic : allFitPaths) {
            Slot slot = backendsWorkingSlots.get(rootPathLoadStatistic.getBeId());
            if (slot == null) {
                LOG.debug("backend {} does not found when getting slot", rootPathLoadStatistic.getBeId());
                continue;
            }

            if (slot.takeSlot(rootPathLoadStatistic.getPathHash()) != -1) {
                return rootPathLoadStatistic;
            }
        }
        return null;
    }

    private void discardTabletInfo(TabletInfo tabletInfo, String reason) {
        tabletInfo.releaseResource(this);
        tabletInfo.setState(org.apache.doris.clone.TabletInfo.State.CANCELLED, reason);
        LOG.info("discard the tablet info because: {}", reason);
    }

    // get at most BATCH_NUM tablets from queue.
    private List<TabletInfo> getNextTabletInfoBatch() {
        List<TabletInfo> list = Lists.newArrayList();
        int count = BATCH_NUM;
        try {
            while (count > 0) {
                TabletInfo tablet = q.poll(1, TimeUnit.SECONDS);
                if (tablet == null) {
                    // no more tablets
                    break;
                }
                list.add(tablet);
                count--;
            }
        } catch (InterruptedException e) {
            LOG.warn("get exception.", e);
        }
        return list;
    }

    public class Slot {
        // path hash -> slot num
        private Map<Long, Integer> pathSlots = Maps.newConcurrentMap();

        public Slot(List<Long> paths, int initSlotNum) {
            for (Long pathHash : paths) {
                pathSlots.put(pathHash, initSlotNum);
            }
        }

        public synchronized void updateSlots(List<Long> paths) {
            // delete non exist path
            Iterator<Map.Entry<Long, Integer>> iter = pathSlots.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, Integer> entry = iter.next();
                if (!paths.contains(entry.getKey())) {
                    iter.remove();
                }
            }

            // add new path
            for (Long pathHash : paths) {
                if (!pathSlots.containsKey(pathHash)) {
                    pathSlots.put(pathHash, SLOT_NUM_PER_PATH);
                }
            }
        }

        /*
         * If the specified 'pathHash' has available slot, decrease the slot number and return this path hash
         */
        public synchronized long takeSlot(long pathHash) {
            Preconditions.checkArgument(pathHash != -1);
            Integer slot = pathSlots.get(pathHash);
            if (slot == null || slot <= 0) {
                return -1;
            }
            
            pathSlots.put(pathHash, slot - 1);
            return pathHash;
        }

        public synchronized void freeSlot(long pathHash) {
            Integer slot = pathSlots.get(pathHash);
            if (slot == null) {
                return;
            }
            
            pathSlots.put(pathHash, slot + 1);
        }

        public synchronized int peekSlot(long pathHash) {
            Integer slot = pathSlots.get(pathHash);
            return slot == null ? -1 : slot;
        }
    }

}
