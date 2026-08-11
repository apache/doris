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
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TBinlogScanType;
import org.apache.doris.thrift.TRow;

import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class BaseTableStream extends Table {
    public enum StreamScanType {
        APPEND_ONLY,
        MIN_DELTA,
        DETAIL,
        UNKNOWN;
        public static StreamScanType getType(String typeName) {
            if (typeName == null) {
                return UNKNOWN;
            }
            typeName = typeName.toLowerCase();
            switch (typeName) {
                case "append_only":
                    return APPEND_ONLY;
                case "min_delta":
                    return MIN_DELTA;
                case "detail":
                    return DETAIL;
                default:
                    return UNKNOWN;
            }
        }

        public static TBinlogScanType toThrift(StreamScanType streamScanType) {
            switch (streamScanType) {
                case MIN_DELTA:
                    return TBinlogScanType.MIN_DELTA;
                case APPEND_ONLY:
                    return TBinlogScanType.APPEND_ONLY;
                case DETAIL:
                    return TBinlogScanType.DETAIL;
                default:
                    return TBinlogScanType.UNKNOWN;
            }
        }
    }

    private static ImmutableList<TableType> supportedTableTypeList = ImmutableList.of(TableType.OLAP);

    @SerializedName("sct")
    protected StreamScanType streamScanType = StreamScanType.MIN_DELTA;

    @SerializedName("sir")
    protected boolean showInitialRows;

    @SerializedName("bti")
    protected TableStreamBaseTableInfo baseTableInfo;

    @SerializedName("d")
    private boolean disabled;

    @SerializedName("s")
    private boolean stale;

    @SerializedName("sr")
    private String staleReason = "N/A";

    protected volatile TableIf baseTable;

    // for persist
    public BaseTableStream() {
        super(TableType.STREAM);
    }

    public BaseTableStream(long id, String streamName, List<Column> fullSchema, TableIf baseTable) {
        super(id, streamName, TableType.STREAM, fullSchema);
        this.baseTableInfo = new TableStreamBaseTableInfo(baseTable);
        this.baseTable = baseTable;
        this.disabled = false;
        this.stale = false;
    }

    public BaseTableStream(String streamName, List<Column> fullSchema, TableIf baseTable) {
        this(-1, streamName, fullSchema, baseTable);
    }

    public TableIf getBaseTableNullable() {
        if (baseTable == null) {
            baseTable = baseTableInfo.getTableNullable();
        }
        return baseTable;
    }

    public void setProperties(Map<String, String> properties) throws org.apache.doris.common.AnalysisException {
        showInitialRows = PropertyAnalyzer.analyzeBooleanProp(properties,
                PropertyAnalyzer.PROPERTIES_STREAM_SHOW_INITIAL_ROWS,
                false);
        streamScanType = PropertyAnalyzer.analyzeStreamType(properties);
    }

    public String getTableStreamType() {
        return "BASE_STREAM";
    }

    public String getScanTypeString() {
        return streamScanType.name();
    }

    public StreamScanType getStreamScanType() {
        return streamScanType;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    public boolean isStale() {
        return stale;
    }

    public void setStale(boolean stale) {
        this.stale = stale;
    }

    public String getStaleReason() {
        return staleReason;
    }

    public void setStaleReason(String staleReason) {
        this.staleReason = staleReason;
    }

    public static boolean isTableTypeSupported(TableIf tableIf) {
        return supportedTableTypeList.contains(tableIf.getType());
    }

    public void appendProperties(StringBuilder sb) {
        sb.append("\"").append(PropertyAnalyzer.PROPERTIES_STREAM_TYPE)
                .append("\" = \"").append(streamScanType).append("\"");
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_STREAM_SHOW_INITIAL_ROWS)
                .append("\" = \"").append(showInitialRows).append("\"\n");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    // fill table_stream_consumption info
    // @param dataBatch the data batch to fill
    // DB_NAME, STREAM_NAME, STREAM_ID, UNIT, CONSUMPTION_STATUS, LAG, LAST_CONSUMPTION_TIME
    abstract void fillTableStreamConsumptionInfo(List<TRow> dataBatch);

    public <E extends Exception> TableIf getBaseTableOrException(java.util.function.Function<String, E> e)
            throws E {
        TableIf table = getBaseTableNullable();
        if (table == null) {
            throw e.apply(baseTableInfo.getTableName());
        }
        return table;
    }

    public TableIf getBaseTableOrNereidsAnalysisException() throws AnalysisException {
        return getBaseTableOrException(
                t -> new AnalysisException(String.format("Unknown base table '%s'", t)));
    }

    public List<String> getBaseTableFullQualifiers() {
        return baseTableInfo.getFullQualifiers();
    }

    public TableStreamBaseTableInfo getBaseTableInfo() {
        return baseTableInfo;
    }

    public boolean isShowInitialRows() {
        return showInitialRows;
    }

    public abstract void unprotectedCheckStreamUpdate(AbstractTableStreamUpdate update)
            throws UserException;

    public abstract void unprotectedUpdateStreamUpdate(AbstractTableStreamUpdate update, Long ts);
}
