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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.rpc.RpcException;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class OlapTableStream extends BaseStream {

    @SerializedName("offset")
    private long offset;

    // for persist
    public OlapTableStream() {
        super();
    }

    public OlapTableStream(long id, String streamName, List<Column> fullSchema, TableIf baseTable) {
        super(id, streamName, fullSchema, baseTable);
        Preconditions.checkArgument(baseTable instanceof OlapTable);
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

    @Override
    public void setProperties(Map<String, String> properties) throws AnalysisException {
        super.setProperties(properties);
        // set offset according to baseTable
        if (showInitialRows) {
            offset = 0;
        } else {
            try {
                offset = ((OlapTable) baseTable).getVisibleVersion();
            } catch (RpcException e) {
                throw new AnalysisException(e.getMessage(), e);
            }
        }
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public String getOffsetDisplayString() {
        return String.valueOf(offset);
    }

    public static OlapTableStream read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), OlapTableStream.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }
}
