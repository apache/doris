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
import org.apache.doris.common.util.QueryStatisticsFormatter;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QueryStatisticsItem;
import org.apache.doris.thrift.TQueryStatistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/*
 * show proc "/current_queries"
 * the statistics is same as the data in audit log.
 */
public class CurrentQueryStatisticsProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("QueryId").add("ConnectionId").add("Catalog").add("Database").add("User")
        .add("ScanRows").add("ScanBytes").add("ReturnedRows").add("CpuMs")
        .add("MaxPeakMemoryBytes").add("CurrentUsedMemoryBytes").add("WorkloadGroupId")
        .add("ShuffleSendBytes").add("ShuffleSendRows")
        .add("ScanBytesFromLocalStorage").add("ScanBytesFromRemoteStorage")
        .add("SpillWriteBytesToLocalStorage").add("SpillReadBytesFromLocalStorage")
        .add("BytesWriteIntoCache").build();

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String name) throws AnalysisException {
        throw new AnalysisException("operation doesn't support.");
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        final BaseProcResult result = new BaseProcResult();
        final Map<String, QueryStatisticsItem> statistic =
                QeProcessorImpl.INSTANCE.getQueryStatistics();
        result.setNames(TITLE_NAMES.asList());
        final List<List<String>> rowData = Lists.newArrayList();
        for (QueryStatisticsItem item : statistic.values()) {
            final List<String> values = Lists.newArrayList();
            final TQueryStatistics queryStatistics = item.getQueryStatistics();
            values.add(item.getQueryId());
            values.add(item.getConnId());
            values.add(item.getCatalog());
            values.add(item.getDb());
            values.add(item.getUser());
            values.add(QueryStatisticsFormatter.getRowsReturned(queryStatistics.getScanRows()));
            values.add(QueryStatisticsFormatter.getScanBytes(queryStatistics.getScanBytes()));
            values.add(QueryStatisticsFormatter.getRowsReturned(queryStatistics.getReturnedRows()));
            values.add(String.valueOf(queryStatistics.getCpuMs()));
            values.add(QueryStatisticsFormatter.getScanBytes(queryStatistics.getMaxPeakMemoryBytes()));
            values.add(QueryStatisticsFormatter.getScanBytes(queryStatistics.getCurrentUsedMemoryBytes()));
            values.add(String.valueOf(queryStatistics.getWorkloadGroupId()));
            values.add(QueryStatisticsFormatter.getScanBytes(queryStatistics.getShuffleSendBytes()));
            values.add(QueryStatisticsFormatter.getRowsReturned(queryStatistics.getShuffleSendRows()));
            values.add(QueryStatisticsFormatter.getScanBytes(queryStatistics.getScanBytesFromLocalStorage()));
            values.add(QueryStatisticsFormatter.getScanBytes(queryStatistics.getScanBytesFromRemoteStorage()));
            values.add(QueryStatisticsFormatter.getScanBytes(queryStatistics.getSpillWriteBytesToLocalStorage()));
            values.add(QueryStatisticsFormatter.getScanBytes(queryStatistics.getSpillReadBytesFromLocalStorage()));
            values.add(QueryStatisticsFormatter.getScanBytes(queryStatistics.getBytesWriteIntoCache()));
            rowData.add(values);
        }
        result.setRows(rowData);
        return result;
    }
}
