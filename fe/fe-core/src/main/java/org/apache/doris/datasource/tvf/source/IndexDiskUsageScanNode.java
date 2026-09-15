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

package org.apache.doris.datasource.tvf.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.tablefunction.IndexDiskUsageTableValuedFunction;
import org.apache.doris.tablefunction.IndexDiskUsageTableValuedFunction.TabletTarget;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Scan node of index_disk_usage. Each tablet is read by one backend that holds it, and every
 * backend gets a single scan range carrying only its own tablets.
 */
public class IndexDiskUsageScanNode extends MetadataScanNode {

    /**
     * Picks the backend that reads one tablet.
     */
    @FunctionalInterface
    interface BackendSelector {
        long select(TabletTarget target) throws UserException;
    }

    private final IndexDiskUsageTableValuedFunction tvf;
    private final List<TScanRangeLocations> scanRanges = Lists.newArrayList();

    public IndexDiskUsageScanNode(PlanNodeId id, TupleDescriptor desc, IndexDiskUsageTableValuedFunction tvf,
            ScanContext scanContext) {
        super(id, desc, tvf, scanContext);
        this.tvf = tvf;
    }

    @Override
    public void init() throws UserException {
        super.init();
        SystemInfoService systemInfo = Env.getCurrentSystemInfo();
        BackendSelector selector = Config.isCloudMode() ? cloudSelector(systemInfo) : localSelector(systemInfo);
        Map<Long, List<TabletTarget>> groups = groupByBackend(tvf.getTabletTargets(), selector);
        TMetaScanRange template = tvf.getMetaScanRange(Lists.newArrayList());
        scanRanges.clear();
        scanRanges.addAll(buildScanRangeLocations(template, groups, systemInfo::getBackend));
        numNodes = scanRanges.size();
    }

    @Override
    protected void initBackendPolicy() {
        // Tablet replicas decide where this scan runs, so the external file backend policy, which
        // also requires load-available backends, does not apply.
    }

    @Override
    protected void createScanRangeLocations() {
        // Scan ranges are built in init(), where replica selection can report a user error.
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return scanRanges;
    }

    @Override
    public int getNumInstances() {
        return scanRanges.size();
    }

    static Map<Long, List<TabletTarget>> groupByBackend(List<TabletTarget> targets, BackendSelector selector)
            throws UserException {
        Map<Long, List<TabletTarget>> groups = Maps.newLinkedHashMap();
        for (TabletTarget target : targets) {
            groups.computeIfAbsent(selector.select(target), backendId -> Lists.newArrayList()).add(target);
        }
        return groups;
    }

    // Applies the replica rule of OLAP scans: a queryable mix node inside the caller's compute group.
    static Predicate<Backend> queryableIn(ComputeGroup computeGroup) {
        boolean invalidComputeGroup = ComputeGroup.INVALID_COMPUTE_GROUP.equals(computeGroup);
        boolean notCloudComputeGroup = computeGroup != null && !Config.isCloudMode();
        return backend -> backend.isQueryAvailable() && backend.isMixNode()
                && !OlapScanNode.shouldFilterReplicaByResourceTag(invalidComputeGroup, notCloudComputeGroup,
                        computeGroup, backend.getLocationTag().value);
    }

    // Spreads tablets over their eligible backends by tablet id, so one backend does not read a
    // whole table while the choice stays deterministic.
    static long chooseBackend(long tabletId, List<Replica> replicas, LongFunction<Backend> backendLookup,
            Predicate<Backend> eligible) throws UserException {
        List<Long> candidates = replicas.stream()
                .map(Replica::getBackendIdWithoutException)
                .filter(backendId -> {
                    Backend backend = backendLookup.apply(backendId);
                    return backend != null && eligible.test(backend);
                })
                .sorted()
                .collect(Collectors.toList());
        if (candidates.isEmpty()) {
            throw new UserException("No queryable replica for tablet " + tabletId);
        }
        return candidates.get((int) Math.floorMod(tabletId, (long) candidates.size()));
    }

    static List<TScanRangeLocations> buildScanRangeLocations(TMetaScanRange template,
            Map<Long, List<TabletTarget>> groups, LongFunction<Backend> backendLookup) {
        // Drop the table-wide tablet list once, so each backend copy only carries its own tablets.
        TMetaScanRange base = template.deepCopy();
        base.getIndexDiskUsageParams().unsetTablets();
        List<TScanRangeLocations> ranges = Lists.newArrayList();
        for (Map.Entry<Long, List<TabletTarget>> group : groups.entrySet()) {
            Backend backend = backendLookup.apply(group.getKey());
            Preconditions.checkState(backend != null, "backend %s is not found", group.getKey());
            TMetaScanRange metaScanRange = base.deepCopy();
            metaScanRange.getIndexDiskUsageParams().setTablets(
                    group.getValue().stream().map(TabletTarget::toThrift).collect(Collectors.toList()));

            TScanRange scanRange = new TScanRange();
            scanRange.setMetaScanRange(metaScanRange);
            TScanRangeLocation location = new TScanRangeLocation();
            location.setBackendId(backend.getId());
            location.setServer(new TNetworkAddress(backend.getHost(), backend.getBePort()));
            TScanRangeLocations locations = new TScanRangeLocations();
            locations.addToLocations(location);
            locations.setScanRange(scanRange);
            ranges.add(locations);
        }
        return ranges;
    }

    private static BackendSelector localSelector(SystemInfoService systemInfo) {
        ConnectContext context = ConnectContext.get();
        Predicate<Backend> eligible = queryableIn(context == null ? null : context.getComputeGroupSafely());
        return target -> {
            Tablet tablet = target.getTablet();
            List<Replica> replicas = tablet.getQueryableReplicas(target.getVersion(),
                    alivePathHashes(tablet, systemInfo), false);
            return chooseBackend(target.getTabletId(), replicas, systemInfo::getBackend, eligible);
        };
    }

    private static BackendSelector cloudSelector(SystemInfoService systemInfo) throws UserException {
        String clusterId = ((CloudSystemInfoService) systemInfo).getCurrentClusterId();
        return target -> {
            for (Replica replica : target.getTablet().getReplicas()) {
                long backendId = ((CloudReplica) replica).getBackendIdWithClusterId(clusterId);
                Backend backend = systemInfo.getBackend(backendId);
                if (backend != null && backend.isQueryAvailable()) {
                    return backendId;
                }
            }
            throw new UserException("No queryable replica for tablet " + target.getTabletId());
        };
    }

    private static Map<Long, Set<Long>> alivePathHashes(Tablet tablet, SystemInfoService systemInfo) {
        Map<Long, Set<Long>> result = Maps.newHashMap();
        for (Replica replica : tablet.getReplicas()) {
            Backend backend = systemInfo.getBackend(replica.getBackendIdWithoutException());
            if (backend != null) {
                result.computeIfAbsent(backend.getId(), id -> backend.getDisks().values().stream()
                        .filter(DiskInfo::isAlive)
                        .map(DiskInfo::getPathHash)
                        .collect(Collectors.toSet()));
            }
        }
        return result;
    }
}
