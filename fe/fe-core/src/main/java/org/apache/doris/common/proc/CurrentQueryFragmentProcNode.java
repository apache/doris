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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.QueryStatisticsItem;
import org.apache.doris.common.util.QueryStatisticsFormatter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/*
 * show proc "/current_queries/{query_id}/fragments"
 * set variable "set is_report_success = true" to enable "ScanBytes" and "ProcessRows".
 */
public class CurrentQueryFragmentProcNode implements ProcNodeInterface {
    private static final Logger LOG = LogManager.getLogger(CurrentQueryFragmentProcNode.class);
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("FragmentId").add("InstanceId").add("Host")
            .add("ScanBytes").add("ProcessRows").build();
    private QueryStatisticsItem item;

    public CurrentQueryFragmentProcNode(QueryStatisticsItem item) {
        this.item = item;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        return requestFragmentExecInfos();
    }

    private ProcResult requestFragmentExecInfos() throws AnalysisException {
        final CurrentQueryInfoProvider provider = new CurrentQueryInfoProvider();
        final Collection<CurrentQueryInfoProvider.InstanceStatistics> instanceStatisticsCollection
                = provider.getInstanceStatistics(item);
        final List<List<String>> sortedRowData = Lists.newArrayList();
        for (CurrentQueryInfoProvider.InstanceStatistics instanceStatistics :
                instanceStatisticsCollection) {
            final List<String> rowData = Lists.newArrayList();
            rowData.add(instanceStatistics.getFragmentId());
            rowData.add(instanceStatistics.getInstanceId().toString());
            rowData.add(instanceStatistics.getAddress().toString());
            if (item.getIsReportSucc()) {
                rowData.add(QueryStatisticsFormatter.getScanBytes(
                        instanceStatistics.getScanBytes()));
                rowData.add(QueryStatisticsFormatter.getRowsReturned(
                        instanceStatistics.getRowsReturned()));
            } else {
                rowData.add("N/A");
                rowData.add("N/A");
            }
            sortedRowData.add(rowData);
        }

        // sort according to explain's fragment index
        sortedRowData.sort(new Comparator<List<String>>() {
            @Override
            public int compare(List<String> l1, List<String> l2) {
                return l1.get(0).compareTo(l2.get(0));
            }
        });
        final BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES.asList());
        result.setRows(sortedRowData);
        return result;
    }

}
