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

import org.apache.doris.analysis.OutFileClause;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TResultFileSinkOptions;
import org.apache.doris.thrift.TResultSink;
import org.apache.doris.thrift.TResultSinkType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Result sink that forwards data to
 * 1. the FE data receiver, which result the final query result to user client. Or,
 * 2. files that save the result data
 */
public class ResultSink extends DataSink {
    private final PlanNodeId exchNodeId;
    private TResultSinkType sinkType;
    private String brokerName;
    private TResultFileSinkOptions fileSinkOptions;

    public ResultSink(PlanNodeId exchNodeId, TResultSinkType sinkType) {
        this.exchNodeId = exchNodeId;
        this.sinkType = sinkType;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix + "RESULT SINK\n");
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink result = new TDataSink(TDataSinkType.RESULT_SINK);
        TResultSink tResultSink = new TResultSink();
        tResultSink.setType(sinkType);
        if (fileSinkOptions != null) {
            tResultSink.setFile_options(fileSinkOptions);
        }
        result.setResult_sink(tResultSink);
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

    public boolean isOutputFileSink() {
        return sinkType == TResultSinkType.FILE;
    }

    public boolean needBroker() {
        return !Strings.isNullOrEmpty(brokerName);
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setOutfileInfo(OutFileClause outFileClause) {
        sinkType = TResultSinkType.FILE;
        fileSinkOptions = outFileClause.toSinkOptions();
        brokerName = outFileClause.getBrokerDesc() == null ? null : outFileClause.getBrokerDesc().getName();
    }

    public void setBrokerAddr(String ip, int port) {
        Preconditions.checkNotNull(fileSinkOptions);
        fileSinkOptions.setBroker_addresses(Lists.newArrayList(new TNetworkAddress(ip, port)));
    }
}
