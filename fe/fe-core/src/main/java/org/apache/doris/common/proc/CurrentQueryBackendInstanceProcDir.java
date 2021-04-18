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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.qe.QueryStatisticsItem;
import org.apache.doris.qe.QeProcessorImpl;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/*
 * show proc "/current_backend_instances";
 */
public class CurrentQueryBackendInstanceProcDir implements ProcDirInterface {

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Backend").add("InstanceNum").add("InstanceId").add("ExecTime").build();

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String name) throws AnalysisException {
        throw new AnalysisException(name + " doesn't exist.");
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        return constructQueryInstanceInfo();
    }

    private ProcResult constructQueryInstanceInfo() {

        // statistics of BE instance info
        final Map<String, QueryStatisticsItem> currentQueryMap = QeProcessorImpl.INSTANCE.getQueryStatistics();
        final Map<String, List<RowData>> hostInstances = Maps.newHashMap();
        for (QueryStatisticsItem item : currentQueryMap.values()) {
            for (QueryStatisticsItem.FragmentInstanceInfo info : item.getFragmentInstanceInfos()) {
                final RowData content = new RowData();
                final String address = new StringBuilder(info.getAddress().getHostname())
                        .append(":")
                        .append(info.getAddress().getPort())
                        .toString();
                content.setHost(address);
                content.setInstanceId(DebugUtil.printId(info.getInstanceId()));
                content.setExecTime(item.getQueryExecTime());
                final String hostWithPort = info.getAddress().toString();
                List<RowData> list = hostInstances.get(hostWithPort);
                if (list == null) {
                    list = Lists.newArrayList();
                    hostInstances.put(hostWithPort, list);
                }
                list.add(content);
            }
        }

        // sort according to InstanceNum
        List<List<RowData>> sortedRowData = Lists.newArrayList();
        sortedRowData.addAll(hostInstances.values());
        sortedRowData.sort(new Comparator<List<RowData>>() {
            @Override
            public int compare(List<RowData> l1, List<RowData> l2) {
                return l1.size() <= l2.size() ? 1 : -1;
            }
        });

        final BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES.asList());
        for (List<RowData> itemList : sortedRowData) {
            for (RowData item : itemList) {
                final List<String> rowData = Lists.newArrayList();
                rowData.add(item.getHost());
                rowData.add(String.valueOf(itemList.size()));
                rowData.add(item.getInstanceId());
                rowData.add(item.getExecTime());
                result.addRow(rowData);
            }

        }
        return result;
    }

    private static class RowData {
        private String host;
        private String backendId;
        private String instanceId;
        private String execTime;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public String getBackendId() {
            return backendId;
        }

        public void setBackendId(String backendId) {
            this.backendId = backendId;
        }

        public String getInstanceId() {
            return instanceId;
        }

        public void setInstanceId(String instanceId) {
            this.instanceId = instanceId;
        }

        public String getExecTime() {
            return execTime;
        }

        public void setExecTime(String execTime) {
            this.execTime = execTime;
        }
    }
}
