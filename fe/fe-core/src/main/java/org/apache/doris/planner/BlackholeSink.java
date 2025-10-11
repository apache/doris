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

import org.apache.doris.thrift.TBlackholeSink;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;

/**
 * BlackholeSink is currently used in "warm up select" SQL statements to preload file block caches into tables.
 * It is planned as a terminal sink (like /dev/null),
 * meaning a "black hole" at the end of the execution plan that discards all incoming data.
 */
public class BlackholeSink extends DataSink {
    private final PlanNodeId exchNodeId;

    public BlackholeSink(PlanNodeId exchNodeId) {
        this.exchNodeId = exchNodeId;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix).append("BLACKHOLE SINK\n");
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink result = new TDataSink(TDataSinkType.BLACKHOLE_SINK);
        TBlackholeSink tBlackholeSink = new TBlackholeSink();
        result.setBlackholeSink(tBlackholeSink);
        return result;
    }

    public PlanNodeId getExchNodeId() {
        return exchNodeId;
    }

    @Override
    public DataPartition getOutputPartition() {
        return null;
    }
}

