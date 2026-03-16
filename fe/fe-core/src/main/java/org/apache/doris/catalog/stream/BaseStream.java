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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class BaseStream extends Table {
    public enum StreamConsumeType {
        DEFAULT,
        APPEND_ONLY,
        MIN_DELTA,
        UNKNOWN;
        public static StreamConsumeType getType(String typeName) {
            if (typeName == null) {
                return UNKNOWN;
            }
            typeName = typeName.toLowerCase();
            switch (typeName) {
                case "default":
                    return DEFAULT;
                case "append_only":
                    return APPEND_ONLY;
                case "min_delta":
                    return MIN_DELTA;
                default:
                    return UNKNOWN;
            }
        }
    }

    private static ImmutableList<TableType> supportedTableTypeList = ImmutableList.of(TableType.OLAP);

    @SerializedName("streamType")
    protected StreamConsumeType streamConsumeType = StreamConsumeType.DEFAULT;

    @SerializedName("showInitialRows")
    protected boolean showInitialRows;

    @SerializedName("baseTableInfo")
    protected BaseTableInfo baseTableInfo;

    @SerializedName("disabled")
    private boolean disabled;

    @SerializedName("stale")
    private boolean stale;

    @SerializedName("staleReason")
    private String staleReason = "N/A";

    protected volatile TableIf baseTable;

    public static final String STREAM_CHANGE_TYPE_COL = "__STREAM_CHANGE_TYPE__";
    public static final String STREAM_SEQ_COL = "__STREAM_SEQUENCE__";

    // for persist
    public BaseStream() {
        super(TableType.STREAM);
    }

    public BaseStream(long id, String streamName, List<Column> fullSchema, TableIf baseTable) {
        super(id, streamName, TableType.STREAM, fullSchema);
        this.baseTableInfo = new BaseTableInfo(baseTable);
        this.baseTable = baseTable;
        this.disabled = false;
        this.stale = false;
    }

    public BaseStream(String streamName, List<Column> fullSchema, TableIf baseTable) {
        this(-1, streamName, fullSchema, baseTable);
    }

    public TableIf getBaseTableNullable() {
        if (baseTable == null) {
            baseTable = baseTableInfo.getTableNullable();
        }
        return baseTable;
    }

    public void setProperties(Map<String, String> properties) throws AnalysisException {
        showInitialRows = PropertyAnalyzer.analyzeBooleanProp(properties,
                PropertyAnalyzer.PROPERTIES_STREAM_SHOW_INITIAL_ROWS,
                false);
        streamConsumeType = PropertyAnalyzer.analyzeStreamType(properties);
    }

    public String getStreamType() {
        return "BASE_STREAM";
    }

    public String getConsumeType() {
        return streamConsumeType.name();
    }

    public boolean getShowInitialRows() {
        return this.showInitialRows;
    }

    public void setShowInitialRows(boolean isShowInitialRows) {
        this.showInitialRows = isShowInitialRows;
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

    public String getOffsetDisplayString() {
        return "N/A";
    }

    public void getProperties(StringBuilder sb) {
        sb.append("\"").append(PropertyAnalyzer.PROPERTIES_STREAM_TYPE)
                .append("\" = \"").append(streamConsumeType).append("\"");
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_STREAM_SHOW_INITIAL_ROWS)
                .append("\" = \"").append(showInitialRows).append("\"\n");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }
}
