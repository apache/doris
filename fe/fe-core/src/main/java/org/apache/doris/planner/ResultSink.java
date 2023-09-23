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

package org.apache.doris.planner;

import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFetchOption;
import org.apache.doris.thrift.TResultSink;
import org.apache.doris.thrift.TResultSinkType;

/**
 * Result sink that forwards data to
 * 1. the FE data receiver, which result the final query result to user client. Or,
 * 2. files that save the result data
 */
public class ResultSink extends DataSink {
    private final PlanNodeId exchNodeId;
    // Two phase fetch option
    private TFetchOption fetchOption;

    private TResultSinkType resultSinkType = TResultSinkType.MYSQL_PROTOCAL;

    public ResultSink(PlanNodeId exchNodeId) {
        this.exchNodeId = exchNodeId;
    }

    public ResultSink(PlanNodeId exchNodeId, TResultSinkType resultSinkType) {
        this.exchNodeId = exchNodeId;
        this.resultSinkType = resultSinkType;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix);
        strBuilder.append("V");
        strBuilder.append("RESULT SINK\n");
        if (fetchOption != null) {
            strBuilder.append(prefix).append("   ").append("OPT TWO PHASE\n");
            if (fetchOption.isFetchRowStore()) {
                strBuilder.append(prefix).append("   ").append("FETCH ROW STORE\n");
            }
        }
        strBuilder.append(prefix).append("   ").append(resultSinkType).append("\n");
        return strBuilder.toString();
    }

    public void setFetchOption(TFetchOption fetchOption) {
        this.fetchOption = fetchOption;
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink result = new TDataSink(TDataSinkType.RESULT_SINK);
        TResultSink tResultSink = new TResultSink();
        if (fetchOption != null) {
            tResultSink.setFetchOption(fetchOption);
        }
        tResultSink.setType(resultSinkType);
        result.setResultSink(tResultSink);
        return result;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return exchNodeId;
    }

    @Override
    public DataPartition getOutputPartition() {
        return null;
    }
}
