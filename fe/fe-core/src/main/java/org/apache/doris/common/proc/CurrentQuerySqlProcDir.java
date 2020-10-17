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
import org.apache.doris.qe.QueryStatisticsItem;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/*
 * show proc "/current_queries/{query_id}"
 */
public class CurrentQuerySqlProcDir implements ProcDirInterface {

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Sql").build();

    private final QueryStatisticsItem item;

    public CurrentQuerySqlProcDir(QueryStatisticsItem item) {
        this.item = item;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String name) throws AnalysisException {
        if (Strings.isNullOrEmpty(name)) {
            return  null;
        }

        if (!name.equals("fragments")) {
            throw new AnalysisException(name + " doesn't exist.");
        }

        return new CurrentQueryFragmentProcNode(item);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        final BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES.asList());
        final List<String> values = Lists.newArrayList();
        values.add(item.getSql());
        result.addRow(values);
        return result;
    }
}
