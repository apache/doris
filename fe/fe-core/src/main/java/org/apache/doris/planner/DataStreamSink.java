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
import org.apache.doris.thrift.TDataStreamSink;
import org.apache.doris.thrift.TExplainLevel;

/**
 * Data sink that forwards data to an exchange node.
 */
public class DataStreamSink extends DataSink {
    private final PlanNodeId exchNodeId;

    private DataPartition outputPartition;

    public DataStreamSink(PlanNodeId exchNodeId) {
        this.exchNodeId = exchNodeId;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return exchNodeId;
    }

    @Override
    public DataPartition getOutputPartition() {
        return outputPartition;
    }

    public void setPartition(DataPartition partition) {
        outputPartition = partition;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix + "STREAM DATA SINK\n");
        strBuilder.append(prefix + "  EXCHANGE ID: " + exchNodeId + "\n");
        if (outputPartition != null) {
            strBuilder.append(prefix + "  " + outputPartition.getExplainString(explainLevel));
        }
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink result = new TDataSink(TDataSinkType.DATA_STREAM_SINK);
        TDataStreamSink tStreamSink =
          new TDataStreamSink(exchNodeId.asInt(), outputPartition.toThrift());
        result.setStreamSink(tStreamSink);
        return result;
    }
}
