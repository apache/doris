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

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.OutFileClause;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.util.FileFormatConstants;
import org.apache.doris.common.util.Util;
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
    private String headerType = "";

    private ResultFileSink(PlanNodeId exchNodeId, OutFileClause outFileClause) {
        this.exchNodeId = exchNodeId;
        this.fileSinkOptions = outFileClause.toSinkOptions();
        this.brokerName = outFileClause.getBrokerDesc() == null ? null :
                outFileClause.getBrokerDesc().getName();
        this.storageType = outFileClause.getBrokerDesc() == null ? StorageBackend.StorageType.LOCAL :
                outFileClause.getBrokerDesc().getStorageType();
    }

    //gen header names
    private String genNames(ArrayList<String> headerNames, String columnSeparator, String lineDelimiter) {
        StringBuilder sb = new StringBuilder();
        for (String name : headerNames) {
            sb.append(name).append(columnSeparator);
        }
        String headerName = sb.substring(0, sb.length() - columnSeparator.length());
        headerName += lineDelimiter;
        return headerName;
    }

    public ResultFileSink(PlanNodeId exchNodeId, OutFileClause outFileClause, ArrayList<String> labels) {
        this(exchNodeId, outFileClause);
        if (Util.isCsvFormat(outFileClause.getFileFormatType())) {
            if (outFileClause.getHeaderType().equals(FileFormatConstants.FORMAT_CSV_WITH_NAMES)
                    || outFileClause.getHeaderType().equals(FileFormatConstants.FORMAT_CSV_WITH_NAMES_AND_TYPES)) {
                header = genNames(labels, outFileClause.getColumnSeparator(), outFileClause.getLineDelimiter());
            }
            headerType = outFileClause.getHeaderType();
        }
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
        tResultFileSink.setHeaderType(headerType);
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

    /**
     * Construct a tuple for file status, the tuple schema as following:
     * | FileNumber    | Int     |
     * | TotalRows     | Bigint  |
     * | FileSize      | Bigint  |
     * | URL           | Varchar |
     * | WriteTimeSec  | Varchar |
     * | WriteSpeedKB  | Varchar |
     */
    public static TupleDescriptor constructFileStatusTupleDesc(DescriptorTable descriptorTable) {
        TupleDescriptor resultFileStatusTupleDesc =
                descriptorTable.createTupleDescriptor("result_file_status");
        resultFileStatusTupleDesc.setIsMaterialized(true);
        for (int i = 0; i < OutFileClause.RESULT_COL_NAMES.size(); ++i) {
            SlotDescriptor slotDescriptor = descriptorTable.addSlotDescriptor(resultFileStatusTupleDesc);
            slotDescriptor.setLabel(OutFileClause.RESULT_COL_NAMES.get(i));
            slotDescriptor.setType(OutFileClause.RESULT_COL_TYPES.get(i));
            slotDescriptor.setColumn(new Column(OutFileClause.RESULT_COL_NAMES.get(i),
                    OutFileClause.RESULT_COL_TYPES.get(i)));
            slotDescriptor.setIsMaterialized(true);
            slotDescriptor.setIsNullable(false);
        }
        resultFileStatusTupleDesc.computeStatAndMemLayout();
        return resultFileStatusTupleDesc;
    }
}
