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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.clone.BackendLoadStatistic;
import org.apache.doris.clone.BackendLoadStatistic.Classification;
import org.apache.doris.clone.LoadStatisticForTag;
import org.apache.doris.clone.RootPathLoadStatistic;
import org.apache.doris.clone.SchedException.SubCode;
import org.apache.doris.clone.TabletSchedCtx;
import org.apache.doris.clone.TabletScheduler;
import org.apache.doris.common.Config;
import org.apache.doris.common.proc.DiagnoseProcDir.DiagnoseItem;
import org.apache.doris.common.proc.DiagnoseProcDir.DiagnoseStatus;
import org.apache.doris.common.proc.DiagnoseProcDir.SubProcDir;
import org.apache.doris.common.proc.TabletHealthProcDir.DBTabletStatistic;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
 * show proc "/diagnose/cluster_balance";
 */
public class DiagnoseClusterBalanceProcDir extends SubProcDir {

    private ForkJoinPool taskPool = new ForkJoinPool();

    @Override
    public List<DiagnoseItem> getDiagnoseResult() {
        long now = System.currentTimeMillis();
        long minToMs = 60 * 1000L;

        Env env = Env.getCurrentEnv();
        TabletScheduler tabletScheduler = env.getTabletScheduler();
        List<TabletSchedCtx> pendingTablets = tabletScheduler.getPendingTablets(1);
        List<TabletSchedCtx> runningTablets = tabletScheduler.getRunningTablets(1);
        List<TabletSchedCtx> historyTablets = tabletScheduler.getHistoryTablets(10000);
        long historyLastVisitTime = historyTablets.stream()
                .mapToLong(tablet -> Math.max(tablet.getCreateTime(), tablet.getLastVisitedTime()))
                .max().orElse(-1);
        boolean schedReady = env.getFrontends(null).stream().anyMatch(
                fe -> fe.isAlive() && now >= fe.getLastStartupTime() + 1 * minToMs
                        && (fe.getRole() == FrontendNodeType.MASTER || fe.getRole() == FrontendNodeType.FOLLOWER));
        boolean schedRecent = !pendingTablets.isEmpty() || !runningTablets.isEmpty()
                || historyLastVisitTime >= now - 15 * minToMs;

        List<DiagnoseItem> items = Lists.newArrayList();
        items.add(diagnoseTabletHealth(schedReady, schedRecent));
        DiagnoseItem baseBalance = diagnoseBaseBalance(schedReady, schedRecent);
        items.add(baseBalance);
        items.add(diagnoseDiskBalance(schedReady, schedRecent, baseBalance.status == DiagnoseStatus.OK));
        items.add(diagnoseColocateRebalance(schedReady, schedRecent));
        items.add(diagnoseHistorySched(historyTablets,
                items.stream().allMatch(item -> item.status == DiagnoseStatus.OK)));

        return items;
    }

    private DiagnoseItem diagnoseTabletHealth(boolean schedReady, boolean schedRecent) {
        DiagnoseItem tabletHealth = new DiagnoseItem();
        tabletHealth.name = "Tablet Health";
        tabletHealth.status = DiagnoseStatus.OK;

        Env env = Env.getCurrentEnv();
        List<DBTabletStatistic> statistics = taskPool.submit(() ->
                env.getInternalCatalog().getDbIds().parallelStream()
                    // skip information_schema database
                    .flatMap(id -> Stream.of(id == 0 ? null : env.getInternalCatalog().getDbNullable(id)))
                    .filter(Objects::nonNull).map(DBTabletStatistic::new)
                    // sort by dbName
                    .sorted(Comparator.comparing(db -> db.db.getFullName())).collect(Collectors.toList())
        ).join();

        DBTabletStatistic total = statistics.stream().reduce(new DBTabletStatistic(), DBTabletStatistic::reduce);
        if (total.tabletNum != total.healthyNum) {
            tabletHealth.status = DiagnoseStatus.ERROR;
            tabletHealth.content = String.format("healthy tablet num %s < total tablet num %s",
                    total.healthyNum, total.tabletNum);
            tabletHealth.detailCmd = "show proc \"/cluster_health/tablet_health\";";
            boolean changeWarning = total.unrecoverableNum == 0;
            if (Config.disable_tablet_scheduler) {
                tabletHealth.suggestion = "has disable tablet balance, ensure master fe config: "
                        + "disable_tablet_scheduler = false";
            } else if (!schedReady) {
                tabletHealth.suggestion = "check all fe are ready, and then wait some minutes for "
                        + "sheduler to migrate tablets";
            } else if (schedRecent) {
                tabletHealth.suggestion = "tablet is still scheduling, run 'show proc \"/cluster_balance\"'";
            } else {
                changeWarning = false;
            }
            if (changeWarning) {
                tabletHealth.status = DiagnoseStatus.WARNING;
            }
        }

        return tabletHealth;
    }

    private DiagnoseItem diagnoseBaseBalance(boolean schedReady, boolean schedRecent) {
        SystemInfoService infoService = Env.getCurrentSystemInfo();
        Map<Tag, LoadStatisticForTag> loadStatisticMap = Env.getCurrentEnv().getTabletScheduler().getStatisticMap();

        DiagnoseItem baseBalance = new DiagnoseItem();
        baseBalance.status = DiagnoseStatus.OK;

        // check base balance
        List<Long> availableBeIds = infoService.getAllBackendIds(true).stream()
                .filter(beId -> infoService.checkBackendScheduleAvailable(beId))
                .collect(Collectors.toList());
        boolean isPartitionBal = Config.tablet_rebalancer_type.equalsIgnoreCase("partition");
        if (isPartitionBal) {
            TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
            baseBalance.name = "Partition Balance";
            List<Integer> tabletNums = availableBeIds.stream()
                    .map(beId -> invertedIndex.getTabletNumByBackendId(beId))
                    .collect(Collectors.toList());
            int minTabletNum = tabletNums.stream().mapToInt(v -> v).min().orElse(0);
            int maxTabletNum = tabletNums.stream().mapToInt(v -> v).max().orElse(0);
            if (maxTabletNum <= Math.max(minTabletNum * Config.diagnose_balance_max_tablet_num_ratio,
                        minTabletNum + Config.diagnose_balance_max_tablet_num_diff)) {
                baseBalance.status = DiagnoseStatus.OK;
            } else {
                baseBalance.status = DiagnoseStatus.ERROR;
                baseBalance.content = String.format("tablets not balance, be %s has %s tablets, be %s has %s tablets",
                        availableBeIds.get(availableBeIds.indexOf(minTabletNum)), minTabletNum,
                        availableBeIds.get(availableBeIds.indexOf(maxTabletNum)), maxTabletNum);
                baseBalance.detailCmd = "show backends";
            }
        } else {
            baseBalance.name = "BeLoad Balance";
            baseBalance.status = DiagnoseStatus.OK;
            OUTER1:
            for (LoadStatisticForTag stat : loadStatisticMap.values()) {
                for (TStorageMedium storageMedium : TStorageMedium.values()) {
                    List<Long> lowBEs = stat.getBackendLoadStatistics().stream()
                            .filter(be -> availableBeIds.contains(be.getBeId())
                                    && be.getClazz(storageMedium) == Classification.LOW)
                            .map(BackendLoadStatistic::getBeId)
                            .collect(Collectors.toList());
                    List<Long> highBEs = stat.getBackendLoadStatistics().stream()
                            .filter(be -> availableBeIds.contains(be.getBeId())
                                    && be.getClazz(storageMedium) == Classification.HIGH)
                            .map(BackendLoadStatistic::getBeId)
                            .collect(Collectors.toList());
                    if (!lowBEs.isEmpty() || !highBEs.isEmpty()) {
                        baseBalance.status = DiagnoseStatus.ERROR;
                        baseBalance.content = String.format("backend load not balance for tag %s, storage medium %s, "
                                + "low load backends %s, high load backends %s",
                                stat.getTag(), storageMedium.name().toUpperCase(), lowBEs, highBEs);
                        baseBalance.detailCmd = String.format("show proc \"/cluster_balance/cluster_load_stat/%s/%s\"",
                                stat.getTag().toKey(), storageMedium.name().toUpperCase());
                        break OUTER1;
                    }
                }
            }
        }

        if (baseBalance.status != DiagnoseStatus.OK) {
            if (Config.disable_tablet_scheduler || Config.disable_balance) {
                baseBalance.suggestion = "has disable tablet balance, ensure master fe config: "
                        + "disable_tablet_scheduler = false, disable_balance = false";
            } else if (!schedReady) {
                baseBalance.suggestion = "check all fe are ready, and then wait some minutes for "
                        + "sheduler to migrate tablets";
                baseBalance.status = DiagnoseStatus.WARNING;
            } else if (schedRecent) {
                baseBalance.suggestion = "tablet is still scheduling, run 'show proc \"/cluster_balance\"'";
                baseBalance.status = DiagnoseStatus.WARNING;
            }
        }

        return baseBalance;
    }


    private DiagnoseItem diagnoseDiskBalance(boolean schedReady, boolean schedRecent, boolean baseBalanceOk) {
        DiagnoseItem diskBalance = new DiagnoseItem();
        diskBalance.name = "Disk Balance";
        diskBalance.status = DiagnoseStatus.OK;

        Map<Tag, LoadStatisticForTag> loadStatisticMap = Env.getCurrentEnv().getTabletScheduler().getStatisticMap();

        OUTER2:
        for (LoadStatisticForTag stat : loadStatisticMap.values()) {
            List<RootPathLoadStatistic> lowPaths = Lists.newArrayList();
            List<RootPathLoadStatistic> midPaths = Lists.newArrayList();
            List<RootPathLoadStatistic> highPaths = Lists.newArrayList();
            for (TStorageMedium storageMedium : TStorageMedium.values()) {
                for (BackendLoadStatistic beStat : stat.getBackendLoadStatistics()) {
                    lowPaths.clear();
                    midPaths.clear();
                    highPaths.clear();
                    beStat.getPathStatisticByClass(lowPaths, midPaths, highPaths, storageMedium);
                    if (!lowPaths.isEmpty() || !highPaths.isEmpty()) {
                        diskBalance.status = DiagnoseStatus.ERROR;
                        diskBalance.content = String.format("backend %s is not disk balance, low paths { %s }, "
                                + "high paths { %s }", beStat.getBeId(),
                                lowPaths.stream().map(RootPathLoadStatistic::getPath).collect(Collectors.toList()),
                                highPaths.stream().map(RootPathLoadStatistic::getPath).collect(Collectors.toList()));
                        diskBalance.detailCmd = String.format(
                                "show proc \"/cluster_balance/cluster_load_stat/%s/%s/%s\"",
                                stat.getTag().toKey(), storageMedium.name().toUpperCase(), beStat.getBeId());
                        break OUTER2;
                    }
                }
            }
        }
        if (diskBalance.status != DiagnoseStatus.OK) {
            if (Config.disable_tablet_scheduler || Config.disable_balance || Config.disable_disk_balance) {
                diskBalance.suggestion = "has disable tablet balance, ensure master fe config: "
                        + "disable_tablet_scheduler = false, disable_balance = false, disable_disk_balance = false";
            } else if (!schedReady) {
                diskBalance.suggestion = "check all fe are ready, and then wait some minutes for "
                        + "sheduler to migrate tablets";
                diskBalance.status = DiagnoseStatus.WARNING;
            } else if (schedRecent) {
                diskBalance.suggestion = "tablet is still scheduling, run 'show proc \"/cluster_balance\"'";
                diskBalance.status = DiagnoseStatus.WARNING;
            } else if (!baseBalanceOk) {
                diskBalance.suggestion = "disk balance run after all be balance, need them finished";
                diskBalance.status = DiagnoseStatus.WARNING;
            }
        }

        return diskBalance;
    }

    private DiagnoseItem diagnoseColocateRebalance(boolean schedReady, boolean schedRecent) {
        DiagnoseItem colocateBalance = new DiagnoseItem();
        colocateBalance.status = DiagnoseStatus.OK;
        colocateBalance.name = "Colocate Group Stable";
        Set<GroupId> unstableGroups = Env.getCurrentEnv().getColocateTableIndex().getUnstableGroupIds();
        if (!unstableGroups.isEmpty()) {
            colocateBalance.status = DiagnoseStatus.ERROR;
            colocateBalance.content = String.format("colocate groups are unstable: %s", unstableGroups);
            colocateBalance.detailCmd = "show proc \"/colocation_group\"";
            if (Config.disable_tablet_scheduler || Config.disable_colocate_balance) {
                colocateBalance.suggestion = "has disable tablet balance, ensure master fe config: "
                        + "disable_tablet_scheduler = false, disable_colocate_balance = false";
            } else if (!schedReady) {
                colocateBalance.suggestion = "check all fe are ready, and then wait some minutes for "
                        + "sheduler to migrate tablets";
                colocateBalance.status = DiagnoseStatus.WARNING;
            } else if (schedRecent) {
                colocateBalance.suggestion = "tablet is still scheduling, run 'show proc \"/cluster_balance\"'";
                colocateBalance.status = DiagnoseStatus.WARNING;
            }
        }

        return colocateBalance;
    }

    private DiagnoseItem diagnoseHistorySched(List<TabletSchedCtx> historyTablets, boolean ignoreErrs) {
        DiagnoseItem historySched = new DiagnoseItem();
        historySched.name = "History Tablet Sched";
        historySched.status = DiagnoseStatus.OK;

        if (!ignoreErrs) {
            long now = System.currentTimeMillis();
            List<TabletSchedCtx> failedTablets = historyTablets.stream()
                    .filter(tablet -> tablet.getLastVisitedTime() >= now - 1800 * 1000L
                            && tablet.getSchedFailedCode() != SubCode.WAITING_SLOT
                            && tablet.getSchedFailedCode() != SubCode.WAITING_DECOMMISSION
                            && tablet.getSchedFailedCode() != SubCode.DIAGNOSE_IGNORE
                            && (tablet.getState() == TabletSchedCtx.State.CANCELLED
                                    || tablet.getState() == TabletSchedCtx.State.UNEXPECTED))
                    .sorted(Comparator.comparing(TabletSchedCtx::getLastVisitedTime).reversed())
                    .limit(5).collect(Collectors.toList());
            if (!failedTablets.isEmpty()) {
                historySched.status = DiagnoseStatus.WARNING;
                historySched.content = String.format("tablet sched has failed: %s", failedTablets.stream()
                        .map(tablet -> String.format("tablet %s error: %s", tablet.getTabletId(), tablet.getErrMsg()))
                        .collect(Collectors.toList()));
                historySched.detailCmd = "show proc \"/cluster_balance/history_tablets\"";
            }
        }

        return historySched;
    }

}
