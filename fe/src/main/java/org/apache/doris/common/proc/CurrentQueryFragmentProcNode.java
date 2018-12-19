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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.QueryStatisticsItem;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.List;

/*
 * show proc "/current_queries/{query_id}/fragments"
 */
public class CurrentQueryFragmentProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("FragmentId").add("InstanceId").add("Host").add("ExecState")
            .add("IO(Running PlanNode)").add("CPU(Running PlanNode)").build();
    private static final Logger LOG = LogManager.getLogger(StmtExecutor.class);
    private QueryStatisticsItem item;

    public CurrentQueryFragmentProcNode(QueryStatisticsItem item) {
        this.item = item;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        return requestFragmentExecInfos();
    }

    private TNetworkAddress toBrpcHost(TNetworkAddress host) throws AnalysisException {
        final Backend backend = Catalog.getCurrentSystemInfo().getBackendWithBePort(
                host.getHostname(), host.getPort());
        if (backend == null) {
            throw new AnalysisException(new StringBuilder("Backend ")
                    .append(host.getHostname())
                    .append(":")
                    .append(host.getPort())
                    .append(" does not exist")
                    .toString());
        }
        if (backend.getBrpcPort() < 0) {
            throw new AnalysisException("BRPC port is't exist.");
        }
        return new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
    }

    private ProcResult requestFragmentExecInfos() throws AnalysisException {

        final CurrentQueryInfoProvider provider = new CurrentQueryInfoProvider();
        final CurrentQueryInfoProvider.QueryExecInfo execInfo = provider.getQueryExecInfoFromRemote(item);
        final List<List<String>> sortedRowDatas = Lists.newArrayList();

        for (CurrentQueryInfoProvider.FragmentExecInfo fragmentExecInfo :
                execInfo.getFragmentExecInfo()) {
            for (CurrentQueryInfoProvider.InstanceExecInfo instanceExecInfo :
                    fragmentExecInfo.getInstanceExecInfo()) {
                long rowNumRead = 0;
                long rowNumProcesed = 0;
                for (CurrentQueryInfoProvider.PlanNodeExecInfo planNodeExecInfo :
                        instanceExecInfo.getPlanNodeExecInfo()) {
                    rowNumRead += planNodeExecInfo.getIoByByte();
                    rowNumProcesed += planNodeExecInfo.getCpuConsumpation();
                }
                final List<String> rowData = Lists.newArrayList();
                rowData.add(fragmentExecInfo.getFragmentId());
                rowData.add(instanceExecInfo.getInstanceId());
                rowData.add(instanceExecInfo.getHost());
                rowData.add(instanceExecInfo.getExecState());
                rowData.add(String.valueOf(rowNumRead));
                rowData.add(String.valueOf(rowNumProcesed));
                sortedRowDatas.add(rowData);
            }
        }
        // sort according to explain's fragment index
        sortedRowDatas.sort(new Comparator<List<String>>() {
            @Override
            public int compare(List<String> l1, List<String> l2) {
                final Integer fragmentId1 = Integer.valueOf(l1.get(0));
                final Integer fragmentId2 = Integer.valueOf(l2.get(0));
                return fragmentId1.compareTo(fragmentId2);
            }
        });
        final BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES.asList());
        result.setRows(sortedRowDatas);
        return result;
    }

}
