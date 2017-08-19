// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.planner;

import com.baidu.palo.catalog.MysqlTable;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.thrift.TDataSink;
import com.baidu.palo.thrift.TExplainLevel;

/**
 * A DataSink describes the destination of a plan fragment's output rows.
 * The destination could be another plan fragment on a remote machine,
 * or a table into which the rows are to be inserted
 * (i.e., the destination of the last fragment of an INSERT statement).
 */
public abstract class DataSink {
    // Fragment that this DataSink belongs to. Set by the PlanFragment enclosing this sink.
    protected PlanFragment fragment_;

    /**
     * Return an explain string for the DataSink. Each line of the explain will be prefixed
     * by "prefix"
     *
     * @param prefix each explain line will be started with the given prefix
     * @return
     */
    public abstract String getExplainString(String prefix, TExplainLevel explainLevel);

    protected abstract TDataSink toThrift();

    public void setFragment(PlanFragment fragment) { fragment_ = fragment; }
    public PlanFragment getFragment() { return fragment_; }

    public abstract PlanNodeId getExchNodeId();

    public abstract DataPartition getOutputPartition();

    public static DataSink createDataSink(Table table) throws AnalysisException {
        if (table instanceof MysqlTable) {
            return new MysqlTableSink((MysqlTable) table);
        } else {
            throw new AnalysisException("Unknown table type " + table.getType());
        }
    }
}
