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
import org.apache.doris.thrift.TMultiCastDataStreamSink;
import org.apache.doris.thrift.TPlanFragmentDestination;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

/**
 * MultiCast data sink.
 */
public class MultiCastDataSink extends DataSink {
    private final List<DataStreamSink> dataStreamSinks = Lists.newArrayList();
    private final List<List<TPlanFragmentDestination>> destinations = Lists.newArrayList();

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder sb = new StringBuilder();

        sb.append(prefix).append("MultiCastDataSinks\n");
        for (DataStreamSink dataStreamSink : dataStreamSinks) {
            sb.append(dataStreamSink.getExplainString(prefix, explainLevel));
        }

        return sb.toString();
    }

    @Override
    protected TDataSink toThrift() {
        List<TDataStreamSink> streamSinkList = dataStreamSinks.stream().map(d -> d.toThrift().getStreamSink())
                .collect(Collectors.toList());
        TMultiCastDataStreamSink sink = new TMultiCastDataStreamSink();
        sink.setSinks(streamSinkList);
        sink.setDestinations(destinations);
        TDataSink result = new TDataSink(TDataSinkType.MULTI_CAST_DATA_STREAM_SINK);
        result.setMultiCastStreamSink(sink);
        return result;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return fragment.getPlanRoot().getId();
    }

    @Override
    public DataPartition getOutputPartition() {
        return fragment.outputPartition;
    }

    public List<DataStreamSink> getDataStreamSinks() {
        return dataStreamSinks;
    }

    public List<List<TPlanFragmentDestination>> getDestinations() {
        return destinations;
    }
}
