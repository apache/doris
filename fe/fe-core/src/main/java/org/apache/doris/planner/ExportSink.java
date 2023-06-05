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

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExportSink;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TNetworkAddress;

import org.apache.commons.lang3.StringEscapeUtils;

public class ExportSink extends DataSink {
    private final String exportPath;
    private final String columnSeparator;
    private final String lineDelimiter;
    private final BrokerDesc brokerDesc;
    private String header = "";

    public ExportSink(String exportPath, String columnSeparator,
                      String lineDelimiter, BrokerDesc brokerDesc) {
        this.exportPath = exportPath;
        this.columnSeparator = columnSeparator;
        this.lineDelimiter = lineDelimiter;
        this.brokerDesc = brokerDesc;
    }

    public ExportSink(String exportPath, String columnSeparator,
                      String lineDelimiter, BrokerDesc brokerDesc, String header) {
        this(exportPath, columnSeparator, lineDelimiter, brokerDesc);
        this.header = header;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix + "EXPORT SINK\n");
        if (explainLevel == TExplainLevel.BRIEF) {
            return sb.toString();
        }
        sb.append(prefix + "  path=" + exportPath + "\n");
        sb.append(prefix + "  columnSeparator="
                + StringEscapeUtils.escapeJava(columnSeparator) + "\n");
        sb.append(prefix + "  lineDelimiter="
                + StringEscapeUtils.escapeJava(lineDelimiter) + "\n");
        sb.append(prefix + "  broker_name=" + brokerDesc.getName() + " property("
                + new PrintableMap<String, String>(
                        brokerDesc.getProperties(), "=", true, false)
                + ")");
        sb.append("\n");
        return sb.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink result = new TDataSink(TDataSinkType.EXPORT_SINK);
        TExportSink tExportSink = new TExportSink(brokerDesc.getFileType(), exportPath, columnSeparator, lineDelimiter);

        if (brokerDesc.getFileType() == TFileType.FILE_BROKER) {
            FsBroker broker = Env.getCurrentEnv().getBrokerMgr().getAnyBroker(brokerDesc.getName());
            if (broker != null) {
                tExportSink.addToBrokerAddresses(new TNetworkAddress(broker.host, broker.port));
            }
        }
        tExportSink.setProperties(brokerDesc.getProperties());
        tExportSink.setHeader(header);
        result.setExportSink(tExportSink);
        return result;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return DataPartition.RANDOM;
    }
}
