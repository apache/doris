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

package org.apache.doris.statistics.query;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Frontend;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TGetQueryStatsRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TQueryStatsResult;
import org.apache.doris.thrift.TQueryStatsType;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTableIndexQueryStats;
import org.apache.doris.thrift.TTableQueryStats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryStatsUtil {
    private static final Logger LOG = LogManager.getLogger(QueryStatsUtil.class);

    public static Map<String, Long> getMergedCatalogStats(String catalog) throws UserException {
        Map<String, Long> result = Env.getCurrentEnv().getQueryStats().getCatalogStats(catalog);
        TGetQueryStatsRequest request = new TGetQueryStatsRequest();
        request.setType(TQueryStatsType.CATALOG);
        request.setCatalog(catalog);
        for (TQueryStatsResult other : getStats(request)) {
            other.getSimpleResult().forEach((k, v) -> {
                result.merge(k, v, Long::sum);
            });
        }
        return result;
    }

    public static Map<String, Long> getMergedDatabaseStats(String catalog, String db) throws UserException {
        Map<String, Long> result = Env.getCurrentEnv().getQueryStats().getDbStats(catalog, db);
        TGetQueryStatsRequest request = new TGetQueryStatsRequest();
        request.setType(TQueryStatsType.DATABASE);
        request.setCatalog(catalog);
        request.setDb(db);
        for (TQueryStatsResult other : getStats(request)) {
            other.getSimpleResult().forEach((k, v) -> {
                result.merge(k, v, Long::sum);
            });
        }
        return result;
    }

    public static Map<String, Pair<Long, Long>> getMergedTableStats(String catalog, String db, String table)
            throws UserException {
        Map<String, Pair<Long, Long>> result = Env.getCurrentEnv().getQueryStats().getTblStats(catalog, db, table);
        TGetQueryStatsRequest request = new TGetQueryStatsRequest();
        request.setType(TQueryStatsType.TABLE);
        request.setCatalog(catalog);
        request.setDb(db);
        request.setTbl(table);
        for (TQueryStatsResult other : getStats(request)) {
            for (TTableQueryStats ts : other.getTableStats()) {
                if (result.containsKey(ts.getField())) {
                    result.get(ts.getField()).first += ts.getQueryStats();
                    result.get(ts.getField()).second += ts.getFilterStats();
                } else {
                    result.put(ts.getField(), Pair.of(ts.getQueryStats(), ts.getFilterStats()));
                }
            }
        }
        return result;
    }

    public static Map<String, Long> getMergedTableAllStats(String catalog, String db, String table)
            throws UserException {
        Map<String, Long> result = Env.getCurrentEnv().getQueryStats().getTblAllStats(catalog, db, table);
        TGetQueryStatsRequest request = new TGetQueryStatsRequest();
        request.setType(TQueryStatsType.TABLE_ALL);
        request.setCatalog(catalog);
        request.setDb(db);
        request.setTbl(table);
        for (TQueryStatsResult other : getStats(request)) {
            other.getSimpleResult().forEach((k, v) -> {
                result.merge(k, v, Long::sum);
            });
        }
        return result;
    }

    public static Map<String, Map<String, Pair<Long, Long>>> getMergedTableAllVerboseStats(String catalog, String db,
            String table) throws UserException {
        Map<String, Map<String, Pair<Long, Long>>> result = Env.getCurrentEnv().getQueryStats()
                .getTblAllVerboseStats(catalog, db, table);
        TGetQueryStatsRequest request = new TGetQueryStatsRequest();
        request.setType(TQueryStatsType.TABLE_ALL_VERBOSE);
        request.setCatalog(catalog);
        request.setDb(db);
        request.setTbl(table);
        for (TQueryStatsResult other : getStats(request)) {
            for (TTableIndexQueryStats tis : other.getTableVerbosStats()) {
                if (result.containsKey(tis.getIndexName())) {
                    for (TTableQueryStats ts : tis.getTableStats()) {
                        if (result.get(tis.getIndexName()).containsKey(ts.getField())) {
                            result.get(tis.getIndexName()).get(ts.getField()).first += ts.getQueryStats();
                            result.get(tis.getIndexName()).get(ts.getField()).second += ts.getFilterStats();
                        } else {
                            result.get(tis.getIndexName())
                                    .put(ts.getField(), Pair.of(ts.getQueryStats(), ts.getFilterStats()));
                        }
                    }
                } else {
                    Map<String, Pair<Long, Long>> indexMap = new HashMap<>();
                    for (TTableQueryStats ts : tis.getTableStats()) {
                        indexMap.put(ts.getField(), Pair.of(ts.getQueryStats(), ts.getFilterStats()));
                    }
                    result.put(tis.getIndexName(), indexMap);
                }
            }
        }
        return result;
    }

    public static long getMergedReplicaStats(long replicaId) {
        long queryHits = Env.getCurrentEnv().getQueryStats().getStats(replicaId);
        TGetQueryStatsRequest request = new TGetQueryStatsRequest();
        request.setType(TQueryStatsType.TABLET);
        request.setReplicaId(replicaId);
        for (TQueryStatsResult other : getStats(request)) {
            queryHits += other.getTabletStats().get(replicaId);
        }
        return queryHits;
    }

    public static Map<Long, Long> getMergedReplicasStats(List<Long> replicaIds) {
        Map<Long, Long> result = new HashMap<>();
        QueryStats qs = Env.getCurrentEnv().getQueryStats();
        for (long replicaId : replicaIds) {
            result.put(replicaId, qs.getStats(replicaId));
        }
        TGetQueryStatsRequest request = new TGetQueryStatsRequest();
        request.setType(TQueryStatsType.TABLETS);
        request.setReplicaIds(replicaIds);
        for (TQueryStatsResult other : getStats(request)) {
            other.getTabletStats().forEach((k, v) -> {
                result.merge(k, v, Long::sum);
            });
        }
        return result;
    }

    private static List<TQueryStatsResult> getStats(TGetQueryStatsRequest request) {
        List<TQueryStatsResult> results = new ArrayList<>();
        for (Frontend fe : Env.getCurrentEnv().getFrontends(null /* all */)) {
            if (!fe.isAlive() || fe.getHost().equals(Env.getCurrentEnv().getSelfNode().getHost())) {
                continue;
            }
            FrontendService.Client client = null;
            try {
                int waitTimeOut = ConnectContext.get() == null ? 300 : ConnectContext.get().getExecTimeout();
                client = ClientPool.frontendPool.borrowObject(new TNetworkAddress(fe.getHost(), fe.getRpcPort()),
                        waitTimeOut * 1000);
                TQueryStatsResult other = client.getQueryStats(request);
                if (!other.isSetStatus() || other.getStatus().getStatusCode() != TStatusCode.OK) {
                    LOG.info("Failed to collect stats from " + fe.getHost());
                    continue;
                }
                switch (request.getType()) {
                    case TABLET:
                    case TABLETS: {
                        if (!other.isSetTabletStats()) {
                            throw new DdlException("Failed to collect stats from " + fe.getHost());
                        }
                        if (other.getTabletStats().isEmpty()) {
                            LOG.info("get empty stats from " + fe.getHost());
                            continue;
                        }
                        break;
                    }
                    case TABLE: {
                        if (!other.isSetTableStats()) {
                            throw new DdlException("Failed to collect stats from " + fe.getHost());
                        }
                        if (other.getTableStats().isEmpty()) {
                            LOG.info("get empty stats from " + fe.getHost());
                            continue;
                        }
                        break;
                    }
                    case CATALOG:
                    case DATABASE:
                    case TABLE_ALL: {
                        if (!other.isSetSimpleResult()) {
                            throw new DdlException("Failed to collect stats from " + fe.getHost());
                        }
                        if (other.getSimpleResult().isEmpty()) {
                            LOG.info("get empty stats from " + fe.getHost());
                            continue;
                        }
                        break;
                    }

                    case TABLE_ALL_VERBOSE: {
                        if (!other.isSetTableVerbosStats()) {
                            throw new DdlException("Failed to collect stats from " + fe.getHost());
                        }
                        if (other.getTableVerbosStats().isEmpty()) {
                            continue;
                        }
                        break;
                    }
                    default: {
                        throw new DdlException("Unknown stats type: " + request.getType());
                    }
                }
                results.add(other);
            } catch (Exception e) {
                LOG.info("Failed to get fe client.", e);
            }
        }
        return results;
    }

}
