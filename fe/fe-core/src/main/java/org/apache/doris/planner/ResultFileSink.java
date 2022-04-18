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

import org.apache.doris.common.FeConstants;
import org.apache.doris.analysis.OutFileClause;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TResultFileSink;
import org.apache.doris.thrift.TResultFileSinkOptions;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;

public class ResultFileSink extends DataSink {
    private PlanNodeId exchNodeId;
    private TResultFileSinkOptions fileSinkOptions;
    private String brokerName;
    private StorageBackend.StorageType storageType;
    private DataPartition outputPartition;
    private TupleId outputTupleId;
    private String header = "";
    private String header_type = "";

    public ResultFileSink(PlanNodeId exchNodeId, OutFileClause outFileClause) {
        this.exchNodeId = exchNodeId;
        this.fileSinkOptions = outFileClause.toSinkOptions();
        this.brokerName = outFileClause.getBrokerDesc() == null ? null :
                outFileClause.getBrokerDesc().getName();
        this.storageType = outFileClause.getBrokerDesc() == null ? StorageBackend.StorageType.LOCAL :
                outFileClause.getBrokerDesc().getStorageType();
    }

    //gen header names 
    private String genNames(ArrayList<String> headerNames, String columnSeparator, String lineDelimiter) {
        String names = "";
        for (String name : headerNames) {
            names += name + columnSeparator;
        }
        names = names.substring(0, names.length() - columnSeparator.length());
        names += lineDelimiter;
        return names;
    }

    public ResultFileSink(PlanNodeId exchNodeId, OutFileClause outFileClause, ArrayList<String> labels) {
        this(exchNodeId, outFileClause);
        if (outFileClause.getHeaderType().equals(FeConstants.csv_with_names) ||
                outFileClause.getHeaderType().equals(FeConstants.csv_with_names_and_types)) {
            header = genNames(labels, outFileClause.getColumnSeparator(), outFileClause.getLineDelimiter());
        }
        header_type = outFileClause.getHeaderType();
    }

    public String getBrokerName() {
        return brokerName;
    }

    public StorageBackend.StorageType getStorageType() {
        return storageType;
    }

    public void setBrokerAddr(String ip, int port) {
        Preconditions.checkNotNull(fileSinkOptions);
        fileSinkOptions.setBrokerAddresses(Lists.newArrayList(new TNetworkAddress(ip, port)));
    }

    public void resetByDataStreamSink(DataStreamSink dataStreamSink) {
        exchNodeId = dataStreamSink.getExchNodeId();
        outputPartition = dataStreamSink.getOutputPartition();
    }

    public void setOutputTupleId(TupleId tupleId) {
        outputTupleId = tupleId;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix);
        strBuilder.append("RESULT FILE SINK\n");
        strBuilder.append("  FILE PATH: " + fileSinkOptions.getFilePath() + "\n");
        strBuilder.append("  STORAGE TYPE: " + storageType.name() + "\n");
        switch (storageType) {
            case BROKER:
                strBuilder.append("  broker name: " + brokerName + "\n");
                break;
            default:
                break;
        }
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink result = new TDataSink(TDataSinkType.RESULT_FILE_SINK);
        TResultFileSink tResultFileSink = new TResultFileSink();
        tResultFileSink.setFileOptions(fileSinkOptions);
        tResultFileSink.setStorageBackendType(storageType.toThrift());
        tResultFileSink.setDestNodeId(exchNodeId.asInt());
        tResultFileSink.setHeaderType(header_type);
        tResultFileSink.setHeader(header);
        if (outputTupleId != null) {
            tResultFileSink.setOutputTupleId(outputTupleId.asInt());
        }
        result.setResultFileSink(tResultFileSink);
        return result;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return exchNodeId;
    }

    @Override
    public DataPartition getOutputPartition() {
        return outputPartition;
    }
}
