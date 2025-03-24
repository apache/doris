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

package org.apache.doris.datasource;

import org.apache.doris.datasource.paimon.source.PaimonSplit;
import org.apache.doris.thrift.TScanRangeLocations;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;

import java.util.List;

public class PaimonSplitProfileInfo extends SplitProfileInfo {
    private final PaimonSplit paimonSplit;

    public PaimonSplitProfileInfo(long weight, PaimonSplit paimonSplit) {
        super(weight);
        this.paimonSplit = paimonSplit;
    }

    @Override
    String getSplitInfo() {
        Split split = paimonSplit.getSplit();
        StringBuilder sb = new StringBuilder();
        if (split == null) {
            sb.append("{")
                    .append(paimonSplit.getPath())
                    .append(",")
                    .append(paimonSplit.getStart())
                    .append(",")
                    .append(paimonSplit.getLength())
                    .append(",")
                    .append(paimonSplit.getFileLength())
                    .append("}");
        } else {
            if (split instanceof DataSplit) {
                List<DataFileMeta> dataFileMetas = ((DataSplit) split).dataFiles();
                boolean first = true;
                for (DataFileMeta dataFileMeta : dataFileMetas) {
                    if (!first) {
                        sb.append(",");
                    }
                    sb.append("{")
                            .append(dataFileMeta.fileName())
                            .append(",")
                            .append(dataFileMeta.rowCount())
                            .append("}");
                    first = false;
                }
            } else {
                return "null";
            }
        }
        return sb.toString();
    }

    public static AssignmentWithSplitInfo<PaimonSplitProfileInfo> create(
            PaimonSplit paimonSplit, TScanRangeLocations scanRangeLocations) {
        return new AssignmentWithSplitInfo<>(new PaimonSplitProfileInfo(
                paimonSplit.getSplitWeight().getRawValue(), paimonSplit), scanRangeLocations);
    }
}
