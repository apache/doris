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
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QueryStatisticsItem;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.List;
import java.util.Map;

/*
 * show proc "/current_query_stmts"
 */
public class CurrentQueryStatementsProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("QueryId").add("ConnectionId").add("Catalog").add("Database").add("User")
            .add("ExecTime").add("SqlHash").add("Statement").build();

    private static final int EXEC_TIME_INDEX = 5;

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        final BaseProcResult result = new BaseProcResult();
        final Map<String, QueryStatisticsItem> statistic =
                QeProcessorImpl.INSTANCE.getQueryStatistics();
        result.setNames(TITLE_NAMES.asList());
        final List<List<String>> sortedRowData = Lists.newArrayList();

        for (QueryStatisticsItem item : statistic.values()) {
            final List<String> values = Lists.newArrayList();
            values.add(item.getQueryId());
            values.add(item.getConnId());
            values.add(item.getCatalog());
            values.add(item.getDb());
            values.add(item.getUser());
            values.add(item.getQueryExecTime());
            values.add(DigestUtils.md5Hex(item.getSql()));
            values.add(item.getSql());
            sortedRowData.add(values);
        }

        // sort according to ExecTime
        sortedRowData.sort((l1, l2) -> {
            final long execTime1 = Long.parseLong(l1.get(EXEC_TIME_INDEX));
            final long execTime2 = Long.parseLong(l2.get(EXEC_TIME_INDEX));
            return Long.compare(execTime2, execTime1);
        });
        result.setRows(sortedRowData);
        return result;
    }
}
