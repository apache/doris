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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ShowAnalyzeTabletsStmt is used to show statistics of tablets info.
 * syntax:
 *    SHOW ANALYZE TABLETS tablet_ids
 */
public class ShowAnalyzeTabletsStmt extends ShowStmt {
    private List<Long> tabletIds;

    private static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("DbName")
            .add("TableName")
            .add("TableSize")
            .add("PartitionNum")
            .add("BucketNum")
            .add("ReplicaCount")
            .add("TabletIds")
            .build();

    public ShowAnalyzeTabletsStmt(List<Long> tabletIds) {
        this.tabletIds = tabletIds;
    }

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (tabletIds == null || tabletIds.isEmpty()) {
            throw new UserException("Please supply at least one tablet id");
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(128)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW ANALYZE TABLETS ");

        for (long tabletId : tabletIds) {
            sb.append(tabletId);
            sb.append(", ");
        }

        String tmp = sb.toString();
        return tmp.substring(tmp.length() - 1);
    }

    @Override
    public String toString() {
        return toSql();
    }
}
