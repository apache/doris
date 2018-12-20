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

package org.apache.doris.qe;


import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.thrift.*;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;

public final class QeProcessorImpl implements QeProcessor {

    private static final Logger LOG = LogManager.getLogger(QeProcessorImpl.class);
    private Map<TUniqueId, QueryInfo> coordinatorMap;

    public static final QeProcessor INSTANCE;

    static {
        INSTANCE = new QeProcessorImpl();
    }

    private QeProcessorImpl() {
        coordinatorMap = Maps.newConcurrentMap();
    }

    @Override
    public void registerQuery(TUniqueId queryId, Coordinator coord) throws UserException {
        registerQuery(queryId, new QueryInfo(coord));
    }

    @Override
    public void registerQuery(TUniqueId queryId, QueryInfo info) throws UserException {
        LOG.info("register query id = " + queryId.toString());
        final QueryInfo result = coordinatorMap.putIfAbsent(queryId, info);
        if (result != null) {
            throw new UserException("queryId " + queryId + " already exists");
        }
    }

    @Override
    public QueryInfo unregisterQuery(TUniqueId queryId) {
        LOG.info("deregister query id = " + queryId.toString());
        return coordinatorMap.remove(queryId);
    }

    @Override
    public Map<String, QueryStatisticsItem> getQueryStatistics() {
        final Map<String, QueryStatisticsItem> querySet = Maps.newHashMap();
        for (Map.Entry<TUniqueId, QueryInfo> entry : coordinatorMap.entrySet()) {
            final QueryInfo info = entry.getValue();
            final ConnectContext context = info.getConnectContext();
            if (info.sql == null || context == null) {
                continue;
            }
            final String queryIdStr = DebugUtil.printId(info.getConnectContext().queryId());
            final QueryStatisticsItem item = new QueryStatisticsItem.Builder()
                    .queryId(queryIdStr)
                    .queryStartTime(info.getStartExecTime())
                    .sql(info.getSql())
                    .user(context.getQualifiedUser())
                    .connId(String.valueOf(context.getConnectionId()))
                    .db(context.getDatabase()).fragmentInstanceInfos(info.getCoord()
                            .getFragmentInstanceInfos()).build();
            querySet.put(queryIdStr, item);
        }
        return querySet;
    }

    @Override
    public TReportExecStatusResult reportExecStatus(TReportExecStatusParams params) {
        LOG.info("ReportExecStatus(): instance_id=" + params.fragment_instance_id.toString()
                + "queryID=" + params.query_id.toString() + " params=" + params);
        final TReportExecStatusResult result = new TReportExecStatusResult();
        final QueryInfo info = coordinatorMap.get(params.query_id);
        if (info == null) {
            result.setStatus(new TStatus(TStatusCode.RUNTIME_ERROR));
            LOG.info("ReportExecStatus() runtime error");
            return result;
        }
        if (params.protocol_version == FrontendServiceVersion.V1) {
            try {
                info.getCoord().updateFragmentExecStatus(params);
            } catch (Exception e) {
                LOG.warn(e.getMessage());
                return result;
            }
        } else if (params.protocol_version == FrontendServiceVersion.V2){
            if (params.getStatus().getStatus_code() == TStatusCode.OK
                    && params.isDone() && params.isSetIo_by_byte() && params.isSetCpu_consumpation()) {
                info.addResourceConsumpation(params.getFragment_instance_id(),
                        params.getIo_by_byte(),
                        params.getCpu_consumpation());
            }
        } else {
            Preconditions.checkState(false);
        }
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    public static final class QueryInfo {

        private final ConnectContext connectContext;
        private final Coordinator coord;
        private final String sql;
        private final long startExecTime;
        private long ioByByte;
        private long cpuConsumpation;
        private Set<TUniqueId> reportInstances;

        // from Export, Pull load, Insert 
        public QueryInfo(Coordinator coord) {
            this(null, null, coord);
        }

        // from query
        public QueryInfo(ConnectContext connectContext, String sql, Coordinator coord) {
            this.connectContext = connectContext;
            this.coord = coord;
            this.sql = sql;
            this.startExecTime = System.currentTimeMillis();
            this.ioByByte = 0;
            this.cpuConsumpation = 0;
            this.reportInstances = Sets.newHashSet();
        }

        public void addResourceConsumpation(TUniqueId instanceId, long io, long cpu) {
            if (!reportInstances.contains(instanceId)) {
                ioByByte += io;
                cpuConsumpation += cpu;
            }
        }

        public long getIoByByte() {
            return ioByByte;
        }

        public long getCpuConsumpation() {
            return cpuConsumpation;
        }

        public ConnectContext getConnectContext() {
            return connectContext;
        }

        public Coordinator getCoord() {
            return coord;
        }

        public String getSql() {
            return sql;
        }

        public long getStartExecTime() {
            return startExecTime;
        }
    }
}
