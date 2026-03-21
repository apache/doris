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

package org.apache.doris.catalog.stream;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OlapTableStream extends BaseStream {

    @SerializedName("po")
    private Map<Long, Long> partitionOffset;

    @SerializedName("pct")
    private Map<Long, Long> partitionConsumptionTime;

    // for persist
    public OlapTableStream() {
        super();
    }

    public OlapTableStream(long id, String streamName, List<Column> fullSchema, TableIf baseTable) {
        super(id, streamName, fullSchema, baseTable);
        Preconditions.checkArgument(baseTable instanceof OlapTable);
        this.partitionOffset = new HashMap<>();
        this.partitionConsumptionTime = new HashMap<>();
        this.baseTable = baseTable;
    }

    public OlapTableStream(String streamName, List<Column> fullSchema, TableIf baseTable) {
        this(-1, streamName, fullSchema, baseTable);
    }

    @Override
    public String getStreamType() {
        return "OLAP_TABLE_STREAM";
    }

    @Override
    public OlapTable getBaseTableNullable() {
        TableIf baseTable = super.getBaseTableNullable();
        if (baseTable == null) {
            return null;
        }
        Preconditions.checkState(baseTable instanceof OlapTable);
        return (OlapTable) baseTable;
    }

    // used for init, should inside base table read lock
    @Override
    public void setProperties(Map<String, String> properties) throws AnalysisException {
        super.setProperties(properties);
        // set offset according to baseTable
        if (!showInitialRows) {
            // set partition offset
            ((OlapTable) baseTable).getPartitions()
                    .forEach(p -> partitionOffset.put(p.getId(), p.getVisibleVersion()));
        }
    }

    public static OlapTableStream read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), OlapTableStream.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    void fillStreamConsumptionInfo(List<TRow> dataBatch) {
        OlapTable table = getBaseTableNullable();
        if (table == null) {
            return;
        }
        if (table.readLockIfExist()) {
            try {
                Map<Long, Partition> id2name = table.getPartitions().stream().collect(Collectors.toMap(
                        p -> p.getId(),
                        p -> p,
                        (oldValue, newValue) -> newValue,
                        HashMap::new
                ));
                for (Map.Entry<Long, Partition> entry : id2name.entrySet()) {
                    TRow trow = new TRow();
                    // DB_NAME
                    trow.addToColumnValue(new TCell().setStringVal(qualifiedDbName));
                    // STREAM_NAME
                    trow.addToColumnValue(new TCell().setStringVal(name));
                    // STREAM_ID
                    trow.addToColumnValue(new TCell().setLongVal(id));
                    // UNIT
                    trow.addToColumnValue(new TCell().setStringVal(entry.getValue().getName()));
                    if (partitionOffset.containsKey(entry.getKey())) {
                        // CONSUMPTION_STATUS
                        trow.addToColumnValue(new TCell()
                                .setStringVal(String.valueOf(partitionOffset.get(entry.getKey()))));
                        // LAG
                        trow.addToColumnValue(new TCell()
                                .setStringVal(String.valueOf(
                                        entry.getValue().getVisibleVersion() - partitionOffset.get(entry.getKey()))));
                        // LAST_CONSUMPTION_TIME
                        if (partitionConsumptionTime.containsKey(entry.getKey())) {
                            trow.addToColumnValue(new TCell()
                                    .setLongVal(partitionConsumptionTime.get(entry.getKey())));
                        } else {
                            trow.addToColumnValue(new TCell().setLongVal(-1));
                        }
                    } else {
                        // CONSUMPTION_STATUS
                        trow.addToColumnValue(new TCell().setStringVal("N/A"));
                        // LAG
                        trow.addToColumnValue(new TCell().setStringVal((String.valueOf(
                                entry.getValue().getVisibleVersion()))));
                        // LAST_CONSUMPTION_TIME
                        trow.addToColumnValue(new TCell().setLongVal(-1));
                    }
                    dataBatch.add(trow);
                }
            } finally {
                table.readUnlock();
            }
        }
    }
}
