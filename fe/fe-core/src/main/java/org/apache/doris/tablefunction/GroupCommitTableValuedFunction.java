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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.planner.GroupCommitScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TFileType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The Implement of table valued function
 * group_commit().
 */
public class GroupCommitTableValuedFunction extends ExternalFileTableValuedFunction {

    public static final String NAME = "group_commit";

    private static final String TABLE_ID_KEY = "table_id";

    private final long tableId;

    public GroupCommitTableValuedFunction(Map<String, String> params) throws AnalysisException {
        if (params == null || !params.containsKey(TABLE_ID_KEY)) {
            throw new AnalysisException(NAME + " should contains property " + TABLE_ID_KEY);
        }
        tableId = Long.parseLong(params.get(TABLE_ID_KEY));
    }

    // =========== implement abstract methods of ExternalFileTableValuedFunction =================

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        List<Column> fileColumns = new ArrayList<>();
        Table table = Env.getCurrentInternalCatalog().getTableByTableId(tableId);
        if (table == null) {
            throw new AnalysisException("table with table_id " + tableId + " is not exists");
        }
        if (!(table instanceof OlapTable)) {
            throw new AnalysisException("Only support OLAP table, but table type of table_id "
                    + tableId + " is " + table.getType());
        }
        if (Config.group_commit_timeout_multipler > 0) {
            int timeoutS = Math.max((int) (((OlapTable) table).getGroupCommitIntervalMs() / 1000.0
                    * Config.group_commit_timeout_multipler), 600);
            ConnectContext.get().getSessionVariable().setInsertTimeoutS(timeoutS);
            ConnectContext.get().getSessionVariable().setQueryTimeoutS(timeoutS);
        }

        List<Column> tableColumns = table.getBaseSchema(true);
        for (int i = 1; i <= tableColumns.size(); i++) {
            fileColumns.add(new Column("c" + i, tableColumns.get(i - 1).getType(), true));
        }
        return fileColumns;
    }

    @Override
    public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc, SessionVariable sv) {
        return new GroupCommitScanNode(id, desc, tableId);
    }

    @Override
    public TFileType getTFileType() {
        return TFileType.FILE_STREAM;
    }

    @Override
    public String getFilePath() {
        return null;
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return new BrokerDesc("GroupCommitTvfBroker", StorageType.STREAM, locationProperties);
    }

    // =========== implement abstract methods of TableValuedFunctionIf =================
    @Override
    public String getTableName() {
        return "GroupCommitTableValuedFunction";
    }
}
